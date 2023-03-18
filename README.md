# cf-deploy
cf-deploy is a package to automate CloudFormation deployments. It simplifies the deployment process by allowing you to use YAML configuration files to define your stack parameters and resources.

Features:
- Merge multiple configuration files to create a single CloudFormation stack
- 

## Installation
To install cf-deploy, run the following command:

```bash
pip install cf-deploy
```
## Usage
To use cf-deploy, create a YAML configuration file with your CloudFormation stack settings. You can use a base configuration file to define common settings and override or extend them with environment-specific configuration files.

Here's a sample base configuration file, base.yaml:

```yaml
region: us-west-2
template: s3://my-bucket/my-template.yaml
deployment_stages:
  - dev
  - prod
```
And a sample environment-specific configuration file, dev.yaml:

```yaml
name: my-stack-dev
prefix: dev-
parameters:
  Stage: dev
  InstanceType: t2.micro
```

## Deploying a CloudFormation stack
To deploy a CloudFormation stack using cf-deploy, run the following command:

```bash
cf-deploy -b base.yaml -c dev.yaml
```

This command will merge the base configuration file (base.yaml) with the environment-specific configuration file (dev.yaml) and deploy the resulting CloudFormation stack.