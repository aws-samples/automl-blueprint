import json
import os

from utils.bpconfig import BPConfig

from sagemaker.s3 import S3Downloader

# the sample v1.0 flow included as an example for this blueprint
FLOW_NAME = "uci-bank-marketing-dataset.flow"
    
def copy_sample_flow_to_local(workspace, local_dir) :

    config = BPConfig.get_config(workspace, local_dir)
        
    fname = f"{local_dir}/{FLOW_NAME}"
    flow_uri = f"s3://{workspace}/{config.ws_prefix()}/meta/{FLOW_NAME}"
    S3Downloader.download(flow_uri, local_dir)

    # Change the flow definition so that it references the dataset copied over by the user
    def _update_sample_flow_def(fname, s3_uri) :
    
        with open(fname, 'r+') as f:
            flow_def = json.loads(f.read())
    
            nodes = flow_def["nodes"]

            for n in nodes :
                if n["type"] == "SOURCE" :
                    data_def = n["parameters"]["dataset_definition"]
                    dstype = data_def["datasetSourceType"]
                    if dstype == "S3" :
                        data_def["s3ExecutionContext"]["s3Uri"] = s3_uri
            f.seek(0)   
            f.write(json.dumps(flow_def))
            f.truncate()
            
    _update_sample_flow_def(fname, config.sample_data_uri())   
     
    return fname