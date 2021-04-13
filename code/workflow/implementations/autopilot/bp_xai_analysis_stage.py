# Author: Dylan Tong, AWS
import json
from time import sleep
from urllib.parse import urlparse

import pandas as pd

import boto3

from sagemaker import clarify
from sagemaker import Session

sm = boto3.client("sagemaker")
s3 = boto3.client("s3")

class TaskTimedOut(Exception): pass

def get_first_matching_s3_key(bucket, prefix=''):
        
    kwargs = {'Bucket': bucket, 'Prefix': prefix, 'MaxKeys': 1}
    resp = s3.list_objects_v2(**kwargs)
    for obj in resp['Contents']:
        return obj['Key']

def get_samples(data_uri, num_samples) :
    
    #obtains sample rows from the first object of many at the specified location
    parsed = urlparse(data_uri, allow_fragments=False)
    if parsed.query:
        prefix= parsed.path.lstrip('/') + '?' + parsed.query
    else:
        prefix= parsed.path.lstrip('/')
        
    key = get_first_matching_s3_key(parsed.netloc,prefix)
    uri = f"s3://{parsed.netloc}/{key}"
    return pd.read_csv(uri,nrows=num_samples)
    
def create_clarify_xai_job(event) :
    
    role = event["Input"]["Payload"]["security-config"]["iam_role"]
    data_params = event["Input"]["Payload"]["data-config"]
    ws_params = event["Input"]["Payload"]["workspace-config"]
    model_params = event["Input"]["Payload"]["model-config"]
    automl_params = event["Input"]["Payload"]["automl-config"]
    xai_params = event["Input"]["Payload"]["xai-config"]
    
    session = Session()
    clarify_processor = clarify.SageMakerClarifyProcessor(role=role,
                                                        instance_count=xai_params["instance_count"],
                                                        instance_type=xai_params["instance_type"],
                                                        sagemaker_session=session)
                                                          
    output_uri = "s3://{}/{}/{}".format(ws_params["s3_bucket"],
                                        ws_params["s3_prefix"],
                                        xai_params["output_prefix"])
    
    shap_params = xai_params["shap-config"]
    num_samples = shap_params["num_samples"]
    df = get_samples(automl_params["data_uri"],shap_params["num_samples"])
    
    samples = df.iloc[:,:-1].values.tolist()
    columns = df.columns.to_list()
    
    shap_config = clarify.SHAPConfig(baseline=samples,
                                    num_samples=num_samples,
                                    agg_method=shap_params["agg_method"])
    
    data_config = clarify.DataConfig(s3_data_input_path=automl_params["data_uri"], 
                                    s3_output_path=output_uri,
                                    label=automl_params["target_name"],
                                    headers=columns,
                                    dataset_type='text/csv')
        
    model_config = clarify.ModelConfig(model_name=model_params["model_name"],
                                    instance_type=model_params["instance_type"],
                                    instance_count=model_params["instance_count"],
                                    content_type='text/csv')
                                        
    clarify_processor.run_explainability(job_name=xai_params["job_name"],
                                    data_config=data_config,
                                    model_config=model_config,
                                    explainability_config=shap_config,
                                    wait=False,
                                    logs=False)

def monitor_job_status(job_name, context) :
    
    sleep_time = 60
    while True: 
    
        status = sm.describe_processing_job(ProcessingJobName = job_name)["ProcessingJobStatus"]

        if status == 'Completed' : 
            break;
                    
        elif status in ('Failed', 'Stopped') :
            break;
        else :
            
            if context.get_remaining_time_in_millis() > 2000*sleep_time :
                sleep(sleep_time)
            else :
                raise TaskTimedOut("Task timed out.")
    
    return {"status":status}


def lambda_handler(event, context):
    
    xai_params = event["Input"]["Payload"]["xai-config"]
    job_name = xai_params["job_name"]
    
    try :
        sm.describe_processing_job(ProcessingJobName = job_name)
    except Exception as e:
        create_clarify_xai_job(event) 
    
    results = monitor_job_status(job_name, context)
    
    event["Input"]["Payload"]["xai-config"]["job-results"] = results
    return event["Input"]["Payload"]