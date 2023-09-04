# This script was sourced from https://github.com/aws-samples/amazon-sagemaker-notebook-instance-lifecycle-config-samples/tree/master/scripts/auto-stop-idle
# This scripts checks if a notebook is idle for X seconds if it does, it'll stop the notebook instance:
# The original script from Amazon expects the caller to pass the idle timeout via parameters,
# but when using CloudFormation we can't make this dynamic as we need to provide a base64 encoded
# string when specifying the content of the start-up script.

from typing import Dict, List, NamedTuple, Tuple
import requests
from datetime import datetime
import getopt, sys
import urllib3
import boto3
import json

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class NotebookResource(NamedTuple):
    arn: str
    name: str


DEFAULT_TAG_KEY = "AutoStopTimeOut"

# Usage
usageInfo = """Usage:
This scripts checks if a notebook is idle for X seconds if it does, it'll stop the notebook:
python autostop.py [--time <time_in_seconds>] [--port <jupyter_port>] [--ignore-connections]
Type "python autostop.py -h" for available options.
"""
# Help info
helpInfo = """-t, --time
    Auto stop time in seconds
-p, --port
    jupyter port
-c --ignore-connections
    Stop notebook once idle, ignore connected users
-h, --help
    Help information
"""

# Read in command-line parameters
idle = True
port = '8443'
ignore_connections = False
default_idle_timeout = 3600
idle_timeout = None
try:
    opts, args = getopt.getopt(sys.argv[1:], "ht:p:c", ["help","time=","port=","ignore-connections"])
    if len(opts) == 0:
        raise getopt.GetoptError("No input parameters!")
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print(helpInfo)
            exit(0)
        if opt in ("-t", "--time"):
            idle_timeout = int(arg)
        if opt in ("-p", "--port"):
            port = str(arg)
        if opt in ("-c", "--ignore-connections"):
            ignore_connections = True
except getopt.GetoptError:
    print(usageInfo)
    exit(1)


def is_idle(last_activity, idle_timeout):
    last_activity = datetime.strptime(last_activity,"%Y-%m-%dT%H:%M:%S.%fz")
    if (datetime.now() - last_activity).total_seconds() > idle_timeout:
        print('Notebook is idle. Last activity time = ', last_activity)
        return True
    else:
        print('Notebook is not idle. Last activity time = ', last_activity)
        return False


def get_notebook_resource() -> NotebookResource:
    metadata_path = '/opt/ml/metadata/resource-metadata.json'
    with open(metadata_path, 'r') as metadata:
        metadata_json = json.load(metadata)
        return NotebookResource(
            arn=metadata_json['ResourceArn'],
            name=metadata_json['ResourceName'])


def get_notebook_timeout_tag(tags: Dict[str, List[str, str]]) -> int:
    for tag in tags:
        if tag['Key'] == DEFAULT_TAG_KEY:
            return int(tag['Value'])


# This is hitting Jupyter's sessions API: https://github.com/jupyter/jupyter/wiki/Jupyter-Notebook-Server-API#Sessions-API
notebook_resource = get_notebook_resource()
response = requests.get(f'https://localhost:{port}/api/sessions', verify=False)
data = response.json()
if len(data) > 0:
    for notebook in data:
        # Idleness is defined by Jupyter
        # https://github.com/jupyter/notebook/issues/4634
        if notebook['kernel']['execution_state'] == 'idle':
            if not ignore_connections:
                if notebook['kernel']['connections'] == 0:
                    if not is_idle(notebook['kernel']['last_activity']):
                        idle = False
                else:
                    idle = False
                    print('Notebook idle state set as %s because no kernel has been detected.' % idle)
            else:
                if not is_idle(notebook['kernel']['last_activity']):
                    idle = False
                    print('Notebook idle state set as %s since kernel connections are ignored.' % idle)
        else:
            print('Notebook is not idle:', notebook['kernel']['execution_state'])
            idle = False
else:
    client = boto3.client('sagemaker')
    uptime = client.describe_notebook_instance(
        NotebookInstanceName=notebook_resource.name
    )['LastModifiedTime']
    tags = client.list_tags(
        ResourceArn=notebook_resource.arn)

    if not idle_timeout:
        idle_timeout = get_notebook_timeout_tag()

    if not idle_timeout:
        idle_timeout = default_idle_timeout

    if not is_idle(uptime.strftime("%Y-%m-%dT%H:%M:%S.%fz"), idle_timeout):
        idle = False
        print('Notebook idle state set as %s since no sessions detected.' % idle)

if idle:
    print('Closing idle notebook')
    client = boto3.client('sagemaker')
    client.stop_notebook_instance(
        NotebookInstanceName=notebook_resource.name
    )
else:
    print('Notebook not idle. Pass.')
