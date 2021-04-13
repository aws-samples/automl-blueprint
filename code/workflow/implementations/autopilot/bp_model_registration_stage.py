#  Author: Dylan Tong, AWS
import json
from time import gmtime, strftime, time

import boto3

from sagemaker import AutoML
from sagemaker import Session

def lambda_handler(event, context):
    
    try :
        model_config = event["Input"]["taskresult"]["Payload"]["model-config"]
        automl_config = event["Input"]["taskresult"]["Payload"]["automl-config"]
        security_config = event["Input"]["taskresult"]["Payload"]["security-config"]
    
        session = Session()
        automl_job = AutoML.attach(automl_config["job_name"],
                                sagemaker_session=session)
                                
        model = automl_job.create_model(model_config["model_name"], 
                                inference_response_keys=model_config["inference_response_keys"])
                                
        model.models[0].env["AUTOML_SPARSE_ENCODE_RECORDIO_PROTOBUF"] = "1"

        session.create_model(name=model_config["model_name"], 
                            role = security_config["iam_role"],
                            container_defs= model.pipeline_container_def(model_config["instance_type"]))
                                        
    except KeyError as e:
        raise KeyError(f"KeyError on input: {event}")
            
    return event["Input"]["taskresult"]["Payload"]