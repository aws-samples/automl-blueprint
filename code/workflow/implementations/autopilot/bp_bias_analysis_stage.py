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

# this is a temporary workaround. There's a bug in the bias detection processing
# script that results in different behavior depending on how the dataset is split.
# the temporary workaround is to merge the files. Be warn that this workaround will
# not scale well.
def create_merged_dataset(s3_src, s3_dst, target_name) :

    parsed = urlparse(s3_src, allow_fragments=False)
    if parsed.query:
        prefix= parsed.path.lstrip('/') + '?' + parsed.query
    else:
        prefix= parsed.path.lstrip('/')
    
    files = []
    kwargs = {'Bucket': parsed.netloc, 'Prefix': prefix, 'MaxKeys': 100}
    resp = s3.list_objects_v2(**kwargs)
    is_first = True
    for obj in resp['Contents'] :
        if is_first :
            files.append(pd.read_csv("s3://{}/{}".format(parsed.netloc, obj["Key"])))
        else :
            files.append(pd.read_csv("s3://{}/{}".format(parsed.netloc, obj["Key"], skiprows=1, header=None)))    

    df = pd.concat(files)
    df[target_name] = df[target_name].astype(int)
    dst = f"{s3_dst}/merged.csv"
    df.to_csv(dst, index=False)
    return dst
    
def get_first_matching_s3_key(bucket, prefix=''):
        
    kwargs = {'Bucket': bucket, 'Prefix': prefix, 'MaxKeys': 1}
    resp = s3.list_objects_v2(**kwargs)
    for obj in resp['Contents']:
        return obj['Key']

def get_data_columns(data_uri) :
    
    #obtains the headers from the first object of many at the specified location
    parsed = urlparse(data_uri, allow_fragments=False)
    if parsed.query:
        prefix= parsed.path.lstrip('/') + '?' + parsed.query
    else:
        prefix= parsed.path.lstrip('/')
    key = get_first_matching_s3_key(parsed.netloc,prefix)
    
    uri = f"s3://{parsed.netloc}/{key}"
    return pd.read_csv(uri,nrows=0).columns.to_list() 

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
    
def create_clarify_bias_job(event) :
    
    role = event["Input"]["Payload"]["security-config"]["iam_role"]
    ws_params = event["Input"]["Payload"]["workspace-config"]
    data_params = event["Input"]["Payload"]["data-config"]
    model_params = event["Input"]["Payload"]["model-config"]
    automl_params = event["Input"]["Payload"]["automl-config"]
    bias_analysis_params = event["Input"]["Payload"]["bias-analysis-config"]
    
    # This is a temporary workaround. The bias detection job behaves differently when
    # files are split. Remove when the bug is fixed.
    ##############################################################################################
    merged_files_dst = "s3://{}/{}/{}".format(  ws_params["s3_bucket"],
                                                ws_params["s3_prefix"],
                                                "data/merged")
    
    input_path = create_merged_dataset(automl_params["data_uri"], merged_files_dst, automl_params["target_name"])
    ################################## End Workaround ############################################

    session = Session()
    clarify_processor = clarify.SageMakerClarifyProcessor(  role=role,
                                                            instance_count=bias_analysis_params["instance_count"],
                                                            instance_type=bias_analysis_params["instance_type"],
                                                            sagemaker_session=session)
                                                          
    output_uri = "s3://{}/{}/{}".format(ws_params["s3_bucket"],
                                        ws_params["s3_prefix"],
                                        bias_analysis_params["output_prefix"])
    
    bias_data_config = clarify.DataConfig(  s3_data_input_path=input_path,
                                            s3_output_path=output_uri,
                                            label=automl_params["target_name"],
                                            headers=get_data_columns(automl_params["data_uri"]),
                                            dataset_type='text/csv')
        
    model_config = clarify.ModelConfig( model_name=model_params["model_name"],
                                        instance_type=model_params["instance_type"],
                                        instance_count=model_params["instance_count"],
                                        accept_type='text/csv',
                                        content_type = 'text/csv')
        
    pred_params = bias_analysis_params["prediction-config"]
    predictions_config = clarify.ModelPredictedLabelConfig( label= pred_params["label"],
                                                            probability= pred_params["probability"],
                                                            probability_threshold=pred_params["probability_threshold"],
                                                            label_headers=pred_params["label_headers"])
        
    bias_params = bias_analysis_params["bias-config"]
    bias_config = clarify.BiasConfig(   label_values_or_threshold=bias_params["label_values_or_threshold"],
                                        facet_name=bias_params["facet_name"],
                                        facet_values_or_threshold=bias_params["facet_values_or_threshold"],
                                        group_name=bias_params["group_name"])   
                                        
    clarify_processor.run_bias( job_name = bias_analysis_params["job_name"],
                                data_config=bias_data_config,
                                bias_config=bias_config,
                                model_config=model_config,
                                model_predicted_label_config=predictions_config,
                                pre_training_methods='all',
                                post_training_methods='all',
                                wait=False,
                                logs=False)                                

def lambda_handler(event, context):

    bias_analysis_params = event["Input"]["Payload"]["bias-analysis-config"]
    job_name = bias_analysis_params["job_name"]
    
    try :
        sm.describe_processing_job(ProcessingJobName = job_name)
    except :
        create_clarify_bias_job(event) 
        
    results = monitor_job_status(job_name, context)
    
    event["Input"]["Payload"]["bias-analysis-config"]["job-results"] = results
    return event["Input"]["Payload"]