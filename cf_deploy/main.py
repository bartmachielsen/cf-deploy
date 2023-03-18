#!/usr/bin/env python

import argparse
import glob
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional, Generator, Iterable, List

import boto3
import botocore
import yaml
from botocore.exceptions import ClientError, ValidationError, WaiterError
from deepmerge import always_merger
from yaml import Loader
from cf_deploy.utils.logging import configure_structlog
import structlog
from cf_deploy.models import Config, BaseConfig


# Configure logging
configure_structlog()
logging.basicConfig(level=logging.INFO)
log = structlog.get_logger("cf-deploy")


def track_stack_events(stack_name, region):
    cf = boto3.client('cloudformation', region_name=region)
    start = time.time()
    seen_event_ids = set()

    while True:
        events = cf.describe_stack_events(StackName=stack_name)

        for event in reversed(events['StackEvents']):
            event_id = event['EventId']
            if event_id not in seen_event_ids and event['Timestamp'].timestamp() > start:
                log.info(
                    "Stack event",
                    timestamp=event['Timestamp'],
                    resource_status=event['ResourceStatus'],
                    resource_type=event['ResourceType'],
                    logical_resource_id=event['LogicalResourceId'],
                )
                seen_event_ids.add(event_id)

        stack = cf.describe_stacks(StackName=stack_name)
        stack_status = stack['Stacks'][0]['StackStatus']

        if stack_status.endswith('_COMPLETE') or stack_status.endswith('_FAILED'):
            break

        time.sleep(5)


def create_change_stack(stack_name, config: Config) -> Optional[str]:
    cf = boto3.client('cloudformation', region_name=config.region)

    # Describe stacks with name
    try:
        stacks = cf.describe_stacks(StackName=stack_name)["Stacks"]
    except ClientError:
        stacks = []

    stack = next((
        stack
        for stack in stacks
        if stack["StackName"] == stack_name
    ), None)

    if stack and "IN_PROGRESS" in stack["StackStatus"] and "REVIEW_IN_PROGRESS" not in stack["StackStatus"]:
        log.warning("Stack is already in progress", name=stack_name)
        track_stack_events(stack_name, config.region)

    # Create change set
    log.info("Creating change set", name=stack_name)
    change_set_response = cf.create_change_set(
        StackName=stack_name,
        TemplateBody=config.template,
        Parameters=[{'ParameterKey': k, 'ParameterValue': str(v)} for k, v in config.parameters.items()],
        Tags=[{'Key': k, 'Value': str(v)} for k, v in {**config.tags, 'Name': stack_name}.items()],
        Capabilities=config.capabilities or [],
        ChangeSetName=f"{stack_name}-changeset-{int(time.time())}",
        ChangeSetType=(
            "UPDATE"
            if stack and stack["StackStatus"] != "DELETE_COMPLETE"
               and stack["StackStatus"] != "REVIEW_IN_PROGRESS"
            else
            "CREATE"
        ),
        IncludeNestedStacks=True,
    )

    # Wait for changeset to be created
    waiter = cf.get_waiter('change_set_create_complete')
    try:
        waiter.wait(
            ChangeSetName=change_set_response['Id']
        )
    except WaiterError as e:
        pass

    # Get Change set
    change_set = cf.describe_change_set(
        StackName=stack_name,
        ChangeSetName=change_set_response['Id'],
    )

    if change_set['Status'] == 'FAILED':
        if "The submitted information didn't contain changes" in change_set['StatusReason']:
            log.info("No changes to deploy", name=stack_name)
            return

        log.error("Change set failed", reason=change_set['StatusReason'])
        return

    # Print changes
    for change in change_set['Changes']:
        log.info(
            "Change",
            resource_type=change['ResourceChange']['ResourceType'],
            action=change['ResourceChange']['Action'],
            replacement=change['ResourceChange'].get("Replacement"),
            logical_resource_id=change['ResourceChange']['LogicalResourceId'],
        )

    return change_set_response['Id']


def deploy_stack(config: Config, arguments):
    stack_name = f"{config.prefix or ''}{config.name}"
    change_set_id = create_change_stack(stack_name, config)

    if arguments.dry_run or not change_set_id:
        return

    if arguments.confirmation_required:
        if input("Do you want to deploy? (y/n)  ") != "y":
            log.info("Aborting deployment")
            return

    log.info("Deploying", name=stack_name)
    cf = boto3.client('cloudformation', region_name=config.region)
    cf.execute_change_set(
        StackName=stack_name,
        ChangeSetName=change_set_id,
    )

    if not arguments.skip_wait:
        track_stack_events(stack_name, config.region)


def loading_config(path: Path, base_config: BaseConfig, arguments) -> Iterable[Config]:
    for p in (path if isinstance(path, list) else [path]):
        for file_location in glob.glob(p):
            with open(file_location, 'r') as config_file:
                log.info("Loading config", file_location=file_location)

                # Validate the config using pydantic
                config = Config(**yaml.load(config_file, Loader=Loader))

                # Resolving references
                for conf_key, conf_val in config.parameters.items():
                    if conf_val.startswith('!') and config.parameters.get(conf_val[1:]):
                        log.info(
                            "Resolving reference",
                            conf_key=conf_key,
                            conf_val=conf_val,
                            resolved_val=config.parameters[conf_val[1:]]
                        )
                        config.parameters[conf_key] = config.parameters[conf_val[1:]]

                # Determining stage
                stage = config.stage
                config.region = config.region or arguments.region

                # Setting default tags
                config.tags = {
                    "Environment": stage,
                    "Name": f"{base_config.prefix or ''}{config.name}",
                    # "Project": arguments.project,
                    "DeployTool": "cf-deploy",
                    **config.tags,
                }

                if config.template.startswith('s3://'):
                    bucket, key = config.template[5:].split('/', 1)
                    log.info("Downloading from S3", template=config.template)
                    s3 = boto3.client('s3', region_name=config.region)
                    response = s3.get_object(Bucket=bucket, Key=key)
                    config.template = response['Body'].read().decode('utf-8')

                yield config


def list_deprecated_stacks(prefix: str, arguments, configs: List[Config]) -> Iterable[str]:
    cf = boto3.client('cloudformation', region_name=arguments.region)

    # Loop all stacks
    paginator = cf.get_paginator('list_stacks')
    for page in paginator.paginate():
        for stack in page["StackSummaries"]:
            if not stack["StackName"].startswith(prefix):
                continue

            if stack["StackStatus"] == "DELETE_COMPLETE":
                continue

            if any(config for config in configs if f"{prefix}{config.name}" == stack["StackName"]):
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

    # Load configs
    if args.parallel:
        log.info("Deploying stacks in parallel")
        with ThreadPoolExecutor(max_workers=10) as executor:
            for config in configs:
                executor.submit(deploy_stack, config, args)
    else:
        for config in configs:
            deploy_stack(config, args)

    if base_config and args.delete_deprecated:
        for stack in list_deprecated_stacks(base_config.prefix, args, configs):
            log.info("Deleting deprecated stack", name=stack)
            cf = boto3.client('cloudformation', region_name=args.region)
            if args.dry_run:
                continue

            if args.confirmation_required:
                if input("Do you want to delete? (y/n)  ") != "y":
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


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.exception(e)
