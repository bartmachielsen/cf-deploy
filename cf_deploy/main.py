#!/usr/bin/env python

import argparse
import dataclasses
import glob
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, Future
from functools import partial, lru_cache
from pathlib import Path
from typing import Optional, Generator, Iterable, List

import boto3
import botocore
import yaml
from botocore.exceptions import ClientError, ValidationError, WaiterError, ConnectionClosedError
from deepmerge import always_merger
from tqdm import tqdm
from yaml import Loader
from cf_deploy.utils.logging import configure_structlog
import structlog
from cf_deploy.models import Config, BaseConfig


# Configure logging
configure_structlog()
logging.basicConfig(level=logging.INFO)
log = structlog.get_logger("cf-deploy")


def generic_constructor(loader, tag_suffix, node):
    if isinstance(node, yaml.ScalarNode):
        return loader.construct_scalar(node)
    elif isinstance(node, yaml.SequenceNode):
        return loader.construct_sequence(node)
    elif isinstance(node, yaml.MappingNode):
        return loader.construct_mapping(node)


yaml.SafeLoader.add_multi_constructor(u'!', generic_constructor)


@dataclasses.dataclass
class StackResourceUpdateFailed(Exception):
    reason: str


def track_stack_events(stack_name, region, verbose=True):
    cf = boto3.client('cloudformation', region_name=region)
    start = time.time()
    seen_events = list()
    seen_event_ids = set()

    while True:
        events = cf.describe_stack_events(StackName=stack_name)

        for event in reversed(events['StackEvents']):
            event_id = event['EventId']
            if event_id not in seen_event_ids and event['Timestamp'].timestamp() > start:
                (log.info if verbose else log.debug)(
                    "Stack event",
                    timestamp=event['Timestamp'],
                    resource_status=event['ResourceStatus'],
                    resource_type=event['ResourceType'],
                    logical_resource_id=event['LogicalResourceId'],
                )
                seen_event_ids.add(event_id)
                seen_events.append(event)

        stack = cf.describe_stacks(StackName=stack_name)
        stack_status = stack['Stacks'][0]['StackStatus']

        if stack_status.endswith('_COMPLETE') or stack_status.endswith('_FAILED'):
            if stack_status.endswith("_FAILED") or stack_status.endswith("_ROLLBACK_COMPLETE"):
                for event in reversed(seen_events):
                    if event['ResourceStatus'].endswith('UPDATE_FAILED'):
                        raise StackResourceUpdateFailed(
                            reason=event["ResourceStatusReason"]
                        )
            break

        time.sleep(10)


def create_change_stack(stack_name, config: Config, base_config: BaseConfig, verbose=True) -> Optional[str]:
    cf = boto3.client('cloudformation', region_name=config.region)

    # Describe stacks with name
    try:
        stacks = cf.describe_stacks(StackName=stack_name)["Stacks"]
    except ClientError:
        stacks = []

    stack = next((
        stack
        for stack in stacks
        if stack["StackName"]==stack_name
    ), None)

    if stack and "IN_PROGRESS" in stack["StackStatus"] and "REVIEW_IN_PROGRESS" not in stack["StackStatus"]:
        (log.info if verbose else log.debug)("Stack is already in progress", name=stack_name)
        track_stack_events(stack_name, config.region)

    # Parse template body
    template_yaml = yaml.safe_load(config.template)
    template_parameter_keys = template_yaml.get("Parameters", {}).keys() if template_yaml and isinstance(template_yaml, dict) else []

    parameters = {**(base_config.parameters if base_config else {}), **config.parameters}

    # Resolving references
    for conf_key, conf_val in parameters.items():
        if str(conf_val).startswith('!'):
            (log.info if verbose else log.debug)(
                "Resolving reference",
                conf_key=conf_key,
                conf_val=conf_val,
                resolved_val=parameters[conf_val[1:]]
            )
            value = parameters[conf_val[1:]]
            parameters[conf_key] = parameters[conf_val[1:]]

    # Create change set
    (log.info if verbose else log.debug)("Creating change set", name=stack_name)
    change_set_response = None
    sleep_backoff = 1
    while True:
        try:
            change_set_response = cf.create_change_set(
                StackName=stack_name,
                TemplateBody=config.template,
                Parameters=[
                    {'ParameterKey': k, 'ParameterValue': str(v)}
                    for k, v in parameters.items()
                    if k in template_parameter_keys or not template_parameter_keys
                ],
                Tags=[{'Key': k, 'Value': str(v)} for k, v in {**(base_config.tags if base_config else {}), **config.tags, 'Name': stack_name}.items()],
                Capabilities=config.capabilities or [],
                ChangeSetName=f"{stack_name}-changeset-{int(time.time())}",
                ChangeSetType=(
                    "UPDATE"
                    if stack and stack["StackStatus"]!="DELETE_COMPLETE"
                       and stack["StackStatus"]!="REVIEW_IN_PROGRESS"
                    else
                    "CREATE"
                ),
                IncludeNestedStacks=True,
            )

        except ClientError as e:
            error = e.response["Error"]
            if error["Code"]=="ValidationError" and "No updates are to be performed" in error["Message"]:
                (log.info if verbose else log.debug)("No changes to deploy", name=stack_name)
                return None

            if "ChangeSet limit exceeded for stack" in error["Message"]:
                log.warning("Deleting change sets, because of ChangeSet limit exceeded", name=stack_name)
                delete_failed_change_sets(stack_name)
                continue

            if "is in UPDATE_ROLLBACK_FAILED state and can not be updated." in error["Message"]:
                log.info("Deleting stack, because of UPDATE_ROLLBACK_FAILED", name=stack_name)
                cf.delete_stack(StackName=stack_name)
                cf.get_waiter('stack_delete_complete').wait(StackName=stack_name)
                log.info("Stack deleted, retrying change stack.", name=stack_name)
                continue

            if "Throttling" in str(e) or "Rate exceeded" in str(e) or "ConnectionClosedError" in str(e):
                log.debug("Throttling, retrying", name=stack_name)
                sleep_backoff = min(sleep_backoff * 2, 60)
                time.sleep(sleep_backoff)
                continue

            log.error(f"Failed to create change set: {error['Message']}", name=stack_name)
            raise e

        if change_set_response:
            break

    # Wait for changeset to be created
    waiter = cf.get_waiter('change_set_create_complete')
    try:
        waiter.wait(
            ChangeSetName=change_set_response['Id']
        )
    except WaiterError:
        pass

    # Get Change set
    sleep_backoff = 1
    change_set = None
    while not change_set:
        try:
            change_set = cf.describe_change_set(
                StackName=stack_name,
                ChangeSetName=change_set_response['Id'],
            )
        except ClientError as e:
            if "Throttling" in str(e) or "Rate exceeded" in str(e) or "ConnectionClosedError" in str(e):
                log.debug("Throttling, retrying", name=stack_name)
                sleep_backoff = min(sleep_backoff * 2, 60)
                time.sleep(sleep_backoff)
                continue
            raise e

        if change_set and change_set["Status"] in ["CREATE_COMPLETE", "CREATE_FAILED", "FAILED"]:
            break

        time.sleep(5)

    if change_set['Status'] == 'FAILED':
        if "The submitted information didn't contain changes" in change_set['StatusReason'] or \
                "No updates are to be performed." in change_set['StatusReason']:
            (log.info if verbose else log.debug)("No changes to deploy", name=stack_name)
            return

        log.error("Change set failed", reason=change_set['StatusReason'])
        return

    # Print changes
    for change in change_set['Changes']:
        (log.info if verbose else log.debug)(
            "Change",
            resource_type=change['ResourceChange']['ResourceType'],
            action=change['ResourceChange']['Action'],
            replacement=change['ResourceChange'].get("Replacement"),
            logical_resource_id=change['ResourceChange']['LogicalResourceId'],
        )

    return change_set_response['Id']


def delete_failed_change_sets(stack_name):
    """
    Delete all failed change sets associated with a given CloudFormation stack.

    Parameters:
    - stack_name: The name of the CloudFormation stack.
    """

    client = boto3.client('cloudformation')

    # Get all change sets for the stack
    response = client.list_change_sets(StackName=stack_name)

    # Filter for failed change sets
    failed_change_sets = [cs for cs in response['Summaries'] if cs['Status']=='FAILED']

    # Delete each failed change set
    sleep_backoff = 1
    for cs in failed_change_sets:
        log.debug(f"Deleting failed change set {cs['ChangeSetId']}...")
        while True:
            try:
                client.delete_change_set(ChangeSetName=cs['ChangeSetId'], StackName=stack_name)
                break
            except ClientError as e:
                if "Throttling" in str(e) or "Rate exceeded" in str(e) or "ConnectionClosedError" in str(e):
                    log.debug("Throttling, retrying", name=stack_name)
                    sleep_backoff = min(sleep_backoff * 2, 60)
                    time.sleep(sleep_backoff)
                    continue

        log.debug(f"Change set {cs['ChangeSetId']} deleted successfully.")


def deploy_stack(stack_name, config: Config, base_config: BaseConfig, arguments, verbose=True):
    cf = boto3.client('cloudformation', region_name=config.region)

    if config.disabled or \
            (base_config and base_config.stage and config.deployment_stages and base_config.stage not in config.deployment_stages):
        try:
            stacks = cf.describe_stacks(StackName=stack_name)["Stacks"]
        except ClientError:
            stacks = []

        if stacks:
            log.info("Deleting", name=stack_name)
            cf.delete_stack(StackName=stack_name)

            if not arguments.skip_wait:
                try:
                    track_stack_events(stack_name, config.region)
                except ClientError as e:
                    if "does not exist" not in str(e):
                        raise e

                    log.info("Stack deleted", name=stack_name)
        return

    change_set_id = create_change_stack(stack_name, config, base_config, verbose=verbose)

    if arguments.dry_run or not change_set_id:
        return

    if arguments.confirmation_required:
        if input("Do you want to deploy? (y/n)  ")!="y":
            log.info("Aborting deployment")
            return

    (log.info if verbose else log.debug)("Deploying", name=stack_name)

    # if config.delete_protected:
    #     try:
    #         stacks = cf.describe_stacks(StackName=stack_name)["Stacks"]
    #     except ClientError:
    #         stacks = []
    #     cf.update_termination_protection(
    #         EnableTerminationProtection=True,
    #         StackName=stack_name
    #     )

    sleep_backoff = 1
    while True:
        try:
            cf.execute_change_set(
                StackName=stack_name,
                ChangeSetName=change_set_id,
            )
            break
        except Exception as e:
            print(str(e))
            if "[CREATE_IN_PROGRESS]" in str(e):
                log.warning("Stack is in CREATE_IN_PROGRESS, waiting", name=stack_name)
                sleep_backoff = min(sleep_backoff * 2, 60)
                time.sleep(sleep_backoff)
                continue
            raise e

    if not arguments.skip_wait:
        try:
            track_stack_events(stack_name, config.region, verbose=verbose)
        except StackResourceUpdateFailed as stack_resource_update_failed_error:
            if "HandlerErrorCode: AlreadyExists" in stack_resource_update_failed_error.reason \
                    and arguments.delete_recreate_on_exists_error:
                log.warning(
                    "Failed to update because of colliding resource, we have to delete stack and recreate", stack_name=stack_name
                )
                log.info("Deleting", name=stack_name)
                cf.delete_stack(StackName=stack_name)
                try:
                    track_stack_events(stack_name, config.region)
                except ClientError as e:
                    if "does not exist" not in str(e):
                        log.exception(e, stack_name=stack_name)
                        raise e
                    raise e

                log.info("Stack deleted", name=stack_name)
                return deploy_stack(stack_name, config, base_config, arguments, verbose)

            log.exception(stack_resource_update_failed_error, stack_name=stack_name)
            raise stack_resource_update_failed_error

    (log.info if verbose else log.debug)("Finished Deployment", name=stack_name)


@lru_cache(25)
def get_template_from_s3(
    s3_arn: str,
    aws_region_name: str
) -> str:
    """Get configuration from S3."""
    bucket, key = s3_arn[5:].split('/', 1)
    log.debug("Downloading from S3", key=key)
    s3 = boto3.client('s3', region_name=aws_region_name)
    response = s3.get_object(Bucket=bucket, Key=key)
    return response['Body'].read().decode('utf-8')



def loading_config(path: Path, base_config: Optional[BaseConfig], arguments) -> Iterable[Config]:
    for p in (path if isinstance(path, list) else [path]):
        for file_location in glob.glob(p, recursive=True):
            with open(file_location, 'r') as config_file:
                log.debug("Loading config", file_location=file_location)

                # Validate the config using pydantic
                config = Config(**yaml.load(config_file, Loader=Loader))

                # Determining stage
                config.region = config.region or arguments.region

                # Setting default tags
                config.tags = {
                    "Name": (
                        f"{base_config.prefix or ''}{config.name}"
                        if base_config else config.name
                    ),
                    # "Project": arguments.project,
                    "DeployTool": "cf-deploy",
                    **config.tags,
                }

                if config.template.startswith('s3://'):
                    config.template = get_template_from_s3(
                        config.template,
                        config.region
                    )

                # Try to open as file path
                if os.path.exists(config.template):
                    with open(config.template, 'r') as template_file:
                        config.template = template_file.read()

                yield config


def list_deprecated_stacks(prefix: str, arguments, configs: List[Config]) -> Iterable[str]:
    cf = boto3.client('cloudformation', region_name=arguments.region)

    # Loop all stacks
    paginator = cf.get_paginator('list_stacks')
    for page in paginator.paginate():
        for stack in page["StackSummaries"]:
            # Skip, is nested stack
            if stack.get("RootId"):
                continue

            if not stack["StackName"].startswith(prefix):
                continue

            if stack["StackStatus"]=="DELETE_COMPLETE":
                continue

            if any(config for config in configs if f"{prefix}{config.name}"==stack["StackName"]):
                continue

            # Get stack tags
            stack_details = cf.describe_stacks(StackName=stack["StackName"])["Stacks"][0]
            stack_tags = {tag["Key"]: tag["Value"] for tag in stack_details["Tags"]}
            if stack_tags.get("DeployTool")!="cf-deploy":
                continue

            yield stack["StackName"]


def main():
    parser = argparse.ArgumentParser(description='Automate CloudFormation deployments')
    parser.add_argument('-c', '--config', required=True, help='Config file or multiple files when using a pattern', nargs='+')
    parser.add_argument('-b', '--base', help='Base configs file to use')
    parser.add_argument("--confirmation-required", help="Ask for confirmation before deploying", action="store_true")
    parser.add_argument("--debug", help="Enable debug logging", action="store_true")
    parser.add_argument("--dry-run", help="Only print changes", action="store_true")
    parser.add_argument("--region", help="AWS region to use", default=os.environ.get("AWS_DEFAULT_REGION", "eu-west-1"))
    parser.add_argument("--skip-wait", help="Disable waiting for stack to be deployed", action="store_true")
    parser.add_argument("--parallel", help="Deploy stacks in parallel", action="store_true")
    parser.add_argument("--delete-deprecated", help="Delete stacks that are not in the config", action="store_true")
    parser.add_argument("--concurrency", help="Number of stacks to deploy in parallel", default=8, type=int)
    parser.add_argument("--delete-recreate-on-exists-error", help="Delete and recreate stack if it already exists", action="store_true")
    parser.add_argument("--delete", help="Delete stacks matching", action="store_true")
    parser.add_argument("--validate", help="Validate stacks matching", action="store_true")

    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    # Parse base configs if given
    base_config: Optional[BaseConfig] = None
    if args.base:
        log.info("Loading base config", base_config_path=args.base)
        with open(args.base, 'r') as base_config_file:
            base_config = BaseConfig(**always_merger.merge(base_config, yaml.load(base_config_file, Loader=Loader)))

    configs: List[Config] = list(loading_config(args.config, base_config, args))

    if args.delete:
        cf = boto3.client('cloudformation', region_name=args.region)
        for config in configs:
            if base_config:
                stack_name = f"{base_config.prefix or ''}{config.name}"
            else:
                stack_name = config.name
            log.info("Deleting stack", name=stack_name)
            cf.delete_stack(StackName=stack_name)
        return

    if args.validate:
        cf = boto3.client('cloudformation', region_name=args.region)
        for config in configs:
            logging.info(f"Validating: {config.name}")
            response = cf.validate_template(
                TemplateBody=config.template
            )
            print(response)
        return

    # Load configs
    if args.parallel:
        try:
            log.info("Deploying stacks in parallel")
            with ThreadPoolExecutor(max_workers=args.concurrency) as executor, tqdm(total=len(configs), desc="Stacks", unit="stack") as progress_bar:
                futures: List[Future] = []
                for config in configs:
                    if base_config:
                        stack_name = f"{base_config.prefix or ''}{config.name}"
                    else:
                        stack_name = config.name
                    futures.append(executor.submit(deploy_stack, stack_name, config, base_config, args, False))

                while futures:
                    for future in list(futures):
                        if future.done():
                            try:
                                future.result()  # This line will raise the exception if the function failed.
                            except Exception as e:
                                log.exception(e)
                                raise e
                            finally:
                                progress_bar.update(1)
                                futures.remove(future)

                    progress_bar.display()
                    time.sleep(0.5)
        except Exception as e:
            log.error(f"An error occurred during the parallel deployment of stacks: {e}")
            raise

    else:
        for config in configs:
            if base_config:
                stack_name = f"{base_config.prefix or ''}{config.name}"
            else:
                stack_name = config.name
            deploy_stack(stack_name, config, base_config, args)

    if base_config and args.delete_deprecated:
        for stack in list_deprecated_stacks(base_config.prefix, args, configs):
            log.info("Deleting deprecated stack", name=stack)
            cf = boto3.client('cloudformation', region_name=args.region)
            if args.dry_run:
                continue

            if args.confirmation_required:
                if input("Do you want to delete? (y/n)  ")!="y":
                    log.info("Aborting deletion")
                    return

            cf.delete_stack(StackName=stack)
            if not args.skip_wait:
                try:
                    track_stack_events(stack, args.region)
                except ClientError as e:
                    if "does not exist" not in str(e):
                        raise e

                    log.info("Stack deleted", name=stack)


if __name__=="__main__":
    try:
        main()
    except Exception as e:
        log.exception(e)
