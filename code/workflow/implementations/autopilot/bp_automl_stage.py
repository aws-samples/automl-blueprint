# Author: Dylan Tong, AWS
import json
import os
from time import gmtime, sleep, strftime, time
from urllib.parse import urlparse

import boto3

class TaskTimedOut(Exception): pass

class AutoMLManager() :

    def __init__(self, drivers=None) :
    
        if not drivers :
            drivers = {
                "s3": boto3.client("s3"),
                "sm" : boto3.client('sagemaker', region_name = os.environ.get('AWS_REGION')),
            }
        
        self.init_drivers(drivers)

    def init_drivers(self, drivers) :
        self.s3 = drivers["s3"] if "s3" in drivers and drivers["s3"] else boto3.client("s3")
        #self.wf = drivers["sfn"] if "sfn" in drivers and drivers["sfn"] else boto3.client("stepfunctions")
        self.dsml = drivers["sm"]if "sm" in drivers and drivers["sm"] else boto3.client('sagemaker', region_name = os.environ.get('AWS_REGION'))

    @classmethod
    def get_job_config(cls, max_candidates) :
            
        return {
                'CompletionCriteria': {
                            'MaxCandidates': int(max_candidates),
                            #'MaxRuntimePerTrainingJobInSeconds': 123,
                            #'MaxAutoMLJobRuntimeInSeconds': 123
                        },
                        #    'VolumeKmsKeyId': 'string',
                        #    'EnableInterContainerTrafficEncryption': True|False,
                        #    'VpcConfig': {
                        #        'SecurityGroupIds': [
                        #            'string',
                        #        ],
                        #        'Subnets': [
                        #            'string',
                        #        ]
                        #    }
                        #}
                    }
                   
    @classmethod
    def get_input_config(cls, data_uri, target) :
                        
        # 'ManifestFile'|'S3Prefix'
        return [{
                    'DataSource': {
                        'S3DataSource': {
                            'S3DataType': 'S3Prefix',
                            'S3Uri': str(data_uri)
                        }
                    },
                    #'CompressionType': 'None'|'Gzip',
                    'TargetAttributeName': target
                },]
        
    def get_automl_objective(metric_name) :    
        return {'MetricName': metric_name}
        
    def get_output_config(output_uri) :
        return {
                    'S3OutputPath': output_uri
                    #'KmsKeyId': 'string'
                }     
    
    SUPPORTED_ENGINES = ["sagemaker-autopilot"]
    @classmethod
    def get_automl_config(cls, event) :
        
        automl_config = event["config"]["Payload"]["automl-config"]
        
        #default to SageMaker Autopilot
        if "engine" not in event["config"]["Payload"]["automl-config"] :
            return cls.generate_autopilot_config(event)
    
        configurator_map = {
            "sagemaker-autopilot": cls.generate_autopilot_config
        }
        
        engine = event["config"]["Payload"]["automl-config"]["engine"]
        if engine in AutoMLManager.SUPPORTED_ENGINES:
            return configurator_map[engine](event)
        else : 
            raise Exception(f"{engine} is not a supported engine. Supported engines are {AutoMLManager.SUPPORTED_ENGINES}.")
            
    @classmethod
    def generate_autopilot_config(cls, event, engine="sagemaker-autopilot") :
    
        try :
            data_config = event["config"]["Payload"]["data-config"]
            security_config = event["config"]["Payload"]["security-config"]
            automl_config = event["config"]["Payload"]["automl-config"]
            ws_config = event["config"]["Payload"]["workspace-config"]
                
            ws_bucket = ws_config["s3_bucket"]
            ws_prefix = ws_config["s3_prefix"]
            
            iam_role = security_config["iam_role"]
            job_name = automl_config["job_name"]
            max_candidates = automl_config["max_candidates"]
            target_name = automl_config["target_name"]
            problem_type = automl_config["problem_type"]
            metric_name = automl_config["metric_name"]
         
            if "automl_max_job_runtime" in automl_config :
                timeout = 600 + automl_config["automl_max_job_runtime"]
            else :
                #limit to 24 hours
                timeout = 86400 
        
            data_uri = event["taskresult"]["ProcessingOutputConfig"]["Outputs"][0]["S3Output"]["S3Uri"]
            config = {
                        "JobName" : job_name,
                        "JobProperties" : cls.get_job_config(max_candidates),
                        "Input" : cls.get_input_config(data_uri, target_name),
                        "Problem" : problem_type,
                        "Objective": cls.get_automl_objective(metric_name),
                        "Output" : cls.get_output_config(f"s3://{ws_bucket}/{ws_prefix}/candidates"),
                        "IamRole" : iam_role,
                        "Region" : os.environ.get('AWS_REGION'),
                        "Timeout" : timeout,
                        "Engine" : engine
                }
        except (KeyError) as e:
            raise type(e)(f"{type(e)}: Event was {event}.")
        
        return config
            
    ## get the first key found under the prefix. We assume that the data
    ## resides in only one output directory.
    @classmethod
    def get_first_matching_s3_key(cls, client, bucket, prefix=''):
            
        kwargs = {'Bucket': bucket, 'Prefix': prefix, 'MaxKeys': 1}
        resp = client.list_objects_v2(**kwargs)
        for obj in resp['Contents']:
            return obj['Key']
            
    ## DT 03/10/20: Datawrangler doesn't write data out to the configured output directory.
    ## It appends child directories that contains information that the user can't obtain.
    ## The workaround involves configuring an output directory with a unique ID to avoid conflicts.
    ## This script then can then "safely" assume where the data is wihtout knowing the exact path.
    @classmethod
    def fix_datawrangler_data_path(cls, client, unique_s3_prefix) :
            
        parsed = urlparse(unique_s3_prefix, allow_fragments=False)
        if parsed.query:
            prefix= parsed.path.lstrip('/') + '?' + parsed.query
        else:
            prefix= parsed.path.lstrip('/')
    
        extended_key = cls.get_first_matching_s3_key(client, parsed.netloc, prefix,)
        return "s3://{}/{}".format(parsed.netloc,"/".join(extended_key.split("/")[:-1]))        
        
    @classmethod
    def monitor_status(cls, job_name, context, client) :
    
        sleep_time = 60
        while True: 
            
            job_description = client.describe_auto_ml_job(AutoMLJobName=job_name)
            status = job_description['AutoMLJobStatus']
            
            results = {"job_name" : job_name}  
            if status == 'Completed' : 
                
                best_candidate = job_description['BestCandidate']
                best_candidate_name = best_candidate['CandidateName']
                
                results["best-candidate"] = {   
                                            "name" : best_candidate_name,
                                            "objective": {
                                                "name": best_candidate['FinalAutoMLJobObjectiveMetric']['MetricName'],
                                                "value": best_candidate['FinalAutoMLJobObjectiveMetric']['Value']
                                                },
                                            "containers" : best_candidate["InferenceContainers"]
                                            }
                
                break;
                        
            elif status in ('Failed', 'Stopped') :
                break;
            else :
                
                if context.get_remaining_time_in_millis() > 2000*sleep_time :
                    sleep(sleep_time)
                else :
                    raise TaskTimedOut("Task timed out.")
        
        results["status"] = status 
        
        return results 
    
    @classmethod
    def model_is_qualified(cls, config, results) :
        
        min_performance = 0
        best_model_performance = -1
        if results["status"] == 'Completed' : 
    
            automl_config = config["config"]["Payload"]["automl-config"]
            min_performance = int(automl_config["minimum_performance"])
            best_model_performance = int(results["best-candidate"]["objective"]["value"])
            
        return best_model_performance >= min_performance
    
    def run_sm_autopilot(self, automl_config, context, wf_state) :
        
        if not context or not wf_state :
            raise Exception("SageMaker Autopilot automl module requires context and wf_state parameters")
        
        try :
            job_def = self.dsml.describe_auto_ml_job(AutoMLJobName=automl_config["JobName"])
        except :
            
            # DT: 03/10/2020 workaround DataWrangler bug P45265671
            unique_s3_prefix = automl_config["Input"][0]["DataSource"]["S3DataSource"]["S3Uri"]
            fixed_data_path = self.fix_datawrangler_data_path(self.s3, unique_s3_prefix)
            automl_config["Input"][0]["DataSource"]["S3DataSource"]["S3Uri"] = fixed_data_path
            
            self.dsml.create_auto_ml_job(AutoMLJobName = automl_config["JobName"],
                                        InputDataConfig = automl_config["Input"],
                                        OutputDataConfig = automl_config["Output"],
                                        AutoMLJobConfig = automl_config["JobProperties"],
                                        ProblemType = automl_config["Problem"],
                                        AutoMLJobObjective = automl_config["Objective"],
                                        RoleArn = automl_config["IamRole"])
    
        results = self.monitor_status(automl_config["JobName"], context, self.dsml)
        results["qualified"] = self.model_is_qualified(wf_state, results)
        
        passed_config = wf_state["config"]["Payload"]
        passed_config["model-config"]["job-results"] = results
        passed_config["automl-config"]["data_uri"] = job_def["InputDataConfig"][0]["DataSource"]["S3DataSource"]["S3Uri"]
        
        return passed_config 
        
    def run_automl(self, automl_config, context=None, wf_state=None) :
        
        configurator_map = {
            "sagemaker-autopilot": self.run_sm_autopilot
        }
        
        if "Engine" in automl_config :
            results = configurator_map[automl_config["Engine"]](automl_config, context, wf_state)
        else :
            raise Exception(f"Bad config {automl_config}. Excepted the key Engine with values: {AutoMLManager.SUPPORTED_ENGINES}")
        
        return results

def lambda_handler(event, context):
    
    automl = AutoMLManager()
    wf_state = event["Input"]
    automl_config = automl.get_automl_config(wf_state)
    
    return automl.run_automl(automl_config, context, wf_state)