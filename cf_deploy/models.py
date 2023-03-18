from collections import defaultdict
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class BaseConfig(BaseModel):
    prefix: Optional[str] = Field(None, description="Prefix to be added to the stack name")
    stage: Optional[str] = Field(None, description="Deployment stage for the stack")
    region: Optional[str] = Field(None, description="AWS region where the stack will be deployed")
    parameters: Optional[Dict[str, Any]] = Field(defaultdict(str), description="Parameters to be passed to the CloudFormation stack")
    tags: Optional[Dict[str, Any]] = Field(defaultdict(str), description="Tags to be added to the CloudFormation stack resources")


class Config(BaseModel):
    name: str = Field(..., description="Name of the CloudFormation stack")
    prefix: Optional[str] = Field(None, description="Prefix to be added to the stack name")
    template: str = Field(..., description="Path to the CloudFormation template file or S3 URL")
    stage: Optional[str] = Field(None, description="Deployment stage for the stack")
    region: Optional[str] = Field(None, description="AWS region where the stack will be deployed")
    parameters: Optional[Dict[str, Any]] = Field(defaultdict(str), description="Parameters to be passed to the CloudFormation stack")
    tags: Optional[Dict[str, Any]] = Field(defaultdict(str), description="Tags to be added to the CloudFormation stack resources")
    deployment_stages: Optional[List[str]] = Field([], description="List of stages where the stack will be deployed")
    capabilities: Optional[List[str]] = Field([], description="List of capabilities needed for the CloudFormation stack")
