# Author: Dylan Tong, AWS
import json
from time import sleep

import boto3

from sagemaker.transformer import Transformer

sm = boto3.client("sagemaker")

class TaskTimedOut(Exception): pass

def create_batch_predictions_job(event) :
    
    role = event["Input"]["Payload"]["security-config"]["iam_role"]
    ws_params = event["Input"]["Payload"]["workspace-config"]
    data_params = event["Input"]["Payload"]["data-config"]
    model_params = event["Input"]["Payload"]["model-config"]
    automl_params = event["Input"]["Payload"]["automl-config"]
    error_analysis_params = event["Input"]["Payload"]["error-analysis-config"]
    xform_params = error_analysis_params["transform-config"]
    
    output_uri = "s3://{}/{}/{}".format(ws_params["s3_bucket"],
                                        ws_params["s3_prefix"],
                                        error_analysis_params["output_prefix"])
                                        
    transformer = Transformer(model_name=model_params["model_name"],
                          instance_count=xform_params["instance_count"],
                          instance_type=xform_params["instance_type"],
                          accept = 'text/csv',
                          strategy=xform_params["strategy"],
                          assemble_with=xform_params["assemble_with"],
                          output_path=output_uri)

    test_data_uri = error_analysis_params["test_data_uri"] if error_analysis_params["test_data_uri"] else automl_params["data_uri"]
    transformer.transform(job_name = error_analysis_params["job_name"],
                        data = test_data_uri,
                        split_type= xform_params["split_type"],
                        content_type= 'text/csv',   
                        input_filter = xform_params["input_filter"],
                        join_source = xform_params["join_source"],
                        output_filter = xform_params["output_filter"],
                        logs=False,
                        wait=False)
    
def monitor_job_status(job_name, context) :
    
    sleep_time = 60
    while True: 
    
        status = sm.describe_transform_job(TransformJobName = job_name)["TransformJobStatus"]

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
    
    error_analysis_params = event["Input"]["Payload"]["error-analysis-config"]
    job_name = error_analysis_params["job_name"]
    
    try :
        sm.describe_transform_job(TransformJobName = job_name)
    except :
        create_batch_predictions_job(event) 
        
    results = monitor_job_status(job_name, context)
    
    event["Input"]["Payload"]["error-analysis-config"]["job-results"] = results
    return event["Input"]["Payload"]