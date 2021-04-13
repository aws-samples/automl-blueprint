# Author: Dylan Tong, AWS
import json
import os
from time import gmtime, strftime, time
import uuid
from urllib.parse import urlparse

import boto3

s3 = boto3.client("s3")
class BlueprintConfig :

    _instance = None

    def __init__(self):
        pass

    @classmethod
    def get_config(cls, event):
        
        if cls._instance is None:
            cls._instance = cls.__new__(cls)

        config_uri = event["Input"]["config_uri"]
        config = get_json_from_s3(config_uri)

        if config :

            cls.dict = config

            if "s3_bucket" in config["workspace-config"] :
                cls.w_bucket = config["workspace-config"]["s3_bucket"]
            else :
                cls.w_bucket = event["default_workspace"]
                cls.dict["workspace-config"]["s3_bucket"] = event["default_workspace"]
            
            cls.w_prefix = config["workspace-config"]["s3_prefix"]

            now = strftime("%Y-%m-%d-%H-%M-%S", gmtime())
            if "s3_bucket" in config["data-config"] :
                cls.d_bucket = config["data-config"]["s3_bucket"]
            else :
                cls.d_bucket = event["default_workspace"]
                cls.dict["data-config"]["s3_bucket"] = event["default_workspace"]

                cls.raw_in_prefix = config["data-config"]["raw_in_prefix"]
                cls.prepped_out_prefix = config["data-config"]["prepped_out_prefix"]

                cls.flow_name = config["dataprep-config"]["definition_file"]
                cls.output_node_id = config["dataprep-config"]["output_node_id"]
                cls.dp_instance_type = config["dataprep-config"]["instance_type"]
                cls.dp_instance_count = int(config["dataprep-config"]["instance_count"])
                cls.dp_data_version = config["dataprep-config"]["data_version"]

                cls.automl_base_job_name = config["automl-config"]["job_base_name"]
                cls.automl_job_name = f"{cls.automl_base_job_name}-{now}"
                cls.automl_max_candidates = int(config["automl-config"]["max_candidates"])
                cls.automl_target_name = config["automl-config"]["target_name"]
                cls.automl_problem_type = config["automl-config"]["problem_type"]
                cls.automl_metric_name = config["automl-config"]["metric_name"]  
                
            if "iam_role" in config["security-config"] :
                cls.iam_role = config["security-config"]["iam_role"]
            else :
                cls.iam_role = event["sm_execution_role"]
                cls.dict["security-config"]["iam_role"] = event["sm_execution_role"]

            cls.model_base_name = config["model-config"]["model_base_name"]
                
            cls.bias_base_name = config["bias-analysis-config"]["job_base_name"]
            cls.xai_base_name = config["xai-config"]["job_base_name"]
            cls.error_base_name = config["error-analysis-config"]["job_base_name"]
                
            ##resource names are initialized here
            cls.dict["automl-config"]["job_name"] = cls.automl_job_name
            cls.dict["model-config"]["model_name"] = f"{cls.model_base_name}-{now}"
            cls.dict["bias-analysis-config"]["job_name"] = f"{cls.bias_base_name}-{now}"
            cls.dict["xai-config"]["job_name"] = f"{cls.xai_base_name}-{now}"
            cls.dict["error-analysis-config"]["job_name"] = f"{cls.error_base_name}-{now}"
    
        else :
            raise Exception("Failed to read config.json.")
                
        return cls._instance
        
class DataWranglerFlowConfig() :
    
    def __init__(self, base_config, name=None):

        guid = f"{strftime('%d-%H-%M-%S', gmtime())}-{str(uuid.uuid4())[:8]}"
        self.name =  name if name else f"dw-process-{guid}"

        self.base_config = base_config
        self.region = boto3.Session().region_name
        self.flow_uri = f"s3://{self.base_config.w_bucket}/{self.base_config.w_prefix}/meta/{self.base_config.flow_name}"
        self.container_uri = self.get_data_prep_container_uri(self.region)

        key = f"{self.base_config.w_prefix}/meta/{self.base_config.flow_name}"
        flow_def= s3.get_object(Bucket=self.base_config.w_bucket, Key=key)['Body']
        self.flow = json.load(flow_def)
        data_uri = f"s3://{self.base_config.d_bucket}/{self.base_config.raw_in_prefix}/"
        self.update_datawrangler_source(self.flow, data_uri)
        
        self.output_name = self.base_config.output_node_id
        self.output_path = self.get_dp_output_path(guid)

    def get_dp_output_path(self, guid) :
        
        return f"s3://{self.base_config.d_bucket}/{self.base_config.prepped_out_prefix}/{guid}"
      

    def get_data_prep_container_uri(self, region):

        image_name = "sagemaker-data-wrangler-container"
        tag = "1.3.0"
        account = {    
            "us-west-1" : "926135532090",
            "us-west-2" : "174368400705",
            "us-east-1" : "663277389841",
            "us-east-2" : "415577184552",
            "ap-east-1" : "707077482487",
            "ap-northeast-1" : "649008135260",
            "ap-northeast-2" : "131546521161",
            "ap-southeast-1" : "119527597002",
            "ap-southeast-2" : "422173101802",
            "ap-south-1" : "089933028263",
            "eu-west-1" : "245179582081",
            "eu-west-2" : "894491911112",
            "eu-west-3" : "807237891255",
            "eu-south-1": "488287956546",
            "eu-central-1" : "024640144536",
            "ca-central-1" : "557239378090",
            "af-south-1" : "143210264188",
            "sa-east-1" : "424196993095",
            "me-south-1" : "376037874950"
           }[region]

        if not account :
            raise Exception("No entry found. Export your flow manually from \
                            the Data Wrangler console and update the account \
                            mapping in this function.")

        return f"{account}.dkr.ecr.{region}.amazonaws.com/{image_name}:{tag}"   
        
    def persist_flow(self) :
        
        s3.put_object(
                    Body= json.dumps(self.flow),
                    Bucket=self.base_config.w_bucket,
                    Key=f"{self.base_config.w_prefix}/meta/{self.base_config.flow_name}")
    
    def update_datawrangler_source(self, flow, data_uri) :
        
        for node in flow["nodes"]:
            if "dataset_definition" in node["parameters"]:
                data_def = node["parameters"]["dataset_definition"]
                source_type = data_def["datasetSourceType"]

                if source_type == "S3":
                    data_def["name"] = "input_data"
                    node["parameters"]["dataset_definition"]["s3ExecutionContext"]["s3Uri"] = data_uri
                    self.persist_flow()
                else:
                    raise ValueError(f"{source_type} support has not been implemented for this blueprint.")
                    
    def create_flow_notebook_processing_input(self, base_dir, flow_s3_uri):
        
        return {
            "InputName": "flow",
            "AppManaged": False,
            "S3Input": {
                "S3Uri": flow_s3_uri,
                'LocalPath': f"{base_dir}/flow",
                'S3DataType': "S3Prefix",
                'S3InputMode': "File",
                'S3DataDistributionType': "FullyReplicated",
                'S3CompressionType': "None"
            }
        }

    def create_s3_processing_input(self, s3_dataset_definition, name, base_dir):
        
        return {
            "InputName": name,
            "AppManaged": False,
            "S3Input": {
                "S3Uri": s3_dataset_definition["s3ExecutionContext"]["s3Uri"],
                'LocalPath': f"{base_dir}/{name}",
                'S3DataType': "S3Prefix",
                'S3InputMode': "File",
                'S3DataDistributionType': "FullyReplicated"
            }
        }

    def create_athena_processing_input(self, athena_dataset_defintion, name, base_dir):
        
        return {
            'InputName': name,
            'AppManaged': False,
            'DatasetDefinition': {
                'AthenaDatasetDefinition': {
                    'Catalog': athena_dataset_defintion["catalogName"],
                    'Database': athena_dataset_defintion["databaseName"],
                    'QueryString': athena_dataset_defintion["queryString"],
                     #'WorkGroup': 'string',
                    'OutputS3Uri': athena_dataset_defintion["s3OutputLocation"] + f"{name}/",
                    'KmsKeyId': 'string',
                    'OutputFormat': athena_dataset_defintion["outputFormat"].upper(),
                    #'OutputCompression': 'GZIP'|'SNAPPY'|'ZLIB'
                },
                'LocalPath': f"{base_dir}/{name}"
            }
        }

    def create_redshift_processing_input(self, redshift_dataset_defintion, name, base_dir):
        
        return {
            'InputName': name,
            'AppManaged': False,
            'RedshiftDatasetDefinition': {
                    'ClusterId': redshift_dataset_defintion["clusterIdentifier"],
                    'Database': redshift_dataset_defintion["database"],
                    'DbUser': redshift_dataset_defintion["dbUser"],
                    'QueryString': redshift_dataset_defintion["queryString"],
                    'ClusterRoleArn': redshift_dataset_defintion["unloadIamRole"],
                    'OutputS3Uri': redshift_dataset_defintion["s3OutputLocation"] + f"{name}/",
                     #'KmsKeyId': 'string',
                    'OutputFormat': redshift_dataset_defintion["outputFormat"].upper(),
                     #'OutputCompression': 'None'|'GZIP'|'BZIP2'|'ZSTD'|'SNAPPY'
                },
                'LocalPath': f"{base_dir}/{name}"
        }

    def create_processing_inputs(self, processing_dir, flow, flow_uri):
        processing_inputs = []
        flow_processing_input = self.create_flow_notebook_processing_input(processing_dir, flow_uri)
        processing_inputs.append(flow_processing_input)

        for node in flow["nodes"]:
            if "dataset_definition" in node["parameters"]:
                data_def = node["parameters"]["dataset_definition"]
                name = data_def["name"]
                source_type = data_def["datasetSourceType"]

                if source_type == "S3":
                    processing_inputs.append(self.create_s3_processing_input(data_def, name, processing_dir))
                elif source_type == "Athena":
                    processing_inputs.append(self.create_athena_processing_input(data_def, name, processing_dir))
                elif source_type == "Redshift":
                    processing_inputs.append(self.create_redshift_processing_input(data_def, name, processing_dir))
                else:
                    raise ValueError(f"{source_type} is not supported for Data Wrangler Processing.")

        return processing_inputs

    def create_processing_output(self, output_name, output_path, processing_dir):
        
        return {
            'OutputName': output_name,
            'S3Output': {
                'S3Uri': output_path,
                'LocalPath': os.path.join(processing_dir, "output"),
                'S3UploadMode': "EndOfJob"
            }
            #'KmsKeyId': 'string'
        }

    def create_container_arguments(self, output_name, output_content_type):
        output_config = {
            output_name: {
                "content_type": output_content_type
            }
        }
        return [f"--output-config '{json.dumps(output_config)}'"]
    
    def get_config(self) :

        processing_dir = "/opt/ml/processing"
        output_content_type = "CSV"
        
        input_dict = []
        inputs = self.create_processing_inputs(processing_dir, self.flow, self.flow_uri)
     
        for inp in inputs : 
            input_dict += [inp]
            
        output_dict = [self.create_processing_output(self.output_name, self.output_path, processing_dir)]
        job_arguments=self.create_container_arguments(self.output_name, output_content_type)
        
        config = {
            "ProcessingJobName": self.name,
            "ClusterConfig": {
                "InstanceType": self.base_config.dp_instance_type,
                "InstanceCount": self.base_config.dp_instance_count,
                "VolumeSizeInGB": 30
            },
            "AppSpecification": {
              "ImageUri": self.container_uri,
              "ContainerArguments": job_arguments,
            },
            "RoleArn": self.base_config.iam_role,
            "ProcessingInputs": input_dict,
            "ProcessingOutputConfig": {
                "Outputs": output_dict
            },
            "StoppingCondition": {
                "MaxRuntimeInSeconds": 86400
            }
        }
             
        return config

def get_json_from_s3(s3obj_uri) :
        
    parsed = urlparse(s3obj_uri, allow_fragments=False)
    if parsed.query:
        prefix= parsed.path.lstrip('/') + '?' + parsed.query
    else:
        prefix= parsed.path.lstrip('/')

    json_object= s3.get_object(Bucket=parsed.netloc, Key=prefix)['Body']
    return json.load(json_object)
    
def get_base_config(config_uri) :
    return get_json_from_s3(config_uri)

def lambda_handler(event, context):

    try :
        base_config = BlueprintConfig().get_config(event)
    except KeyError:
        raise KeyError(f"Incorrect Step Functions input {event}. Expected S3Uri pointing to config file under key config_uri")
    
    dw_config = DataWranglerFlowConfig(base_config).get_config()
    base_config.dict["dataprep-config"]["data-wrangler-job-def"] = dw_config
    
    return base_config.dict