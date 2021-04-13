import json

from sagemaker.s3 import S3Downloader, S3Uploader

def default_config_uri(workspace) :
    return f"s3://{workspace}/automl-blueprint/config/blueprint-config.json"
    
class BPConfig() :
    
    _instance = None

    def __init__(self):
        pass

    @classmethod
    def get_config(cls, bp_workspace, local_wd):
        
        cls._instance = cls.__new__(cls)
        cls.workspace = bp_workspace
        cls.local_dir = local_wd
        cls.config = cls._get_bp_config()
    
        return cls._instance
    
    def print(self, indent=4, sort_keys=True) :
        print(json.dumps(self.config, indent=indent, sort_keys=sort_keys))
    
    @classmethod
    def _download_bp_config(cls, config_uri = None) :

        if not config_uri :
            config_uri = default_config_uri(cls.workspace)
        
        S3Downloader.download(config_uri, cls.local_dir)

        fname = f"{cls.local_dir}/blueprint-config.json"
        return fname

    @classmethod
    def _get_bp_config(cls) :

        fname = cls._download_bp_config()

        with open(fname, 'r') as f:
            config = json.loads(f.read())
            return config
    
    @classmethod
    def _write_to_remote_storage(cls, local, remote) :
        # Currently, only supports Amazon S3
        S3Uploader.upload(local, remote)    
        
    @classmethod 
    def _write_to_local_storage(cls, full_path) :
            
        with open(full_path, 'w') as f:
            f.write(json.dumps(cls.config))
            f.truncate()
        
    @classmethod
    def _persist_bp_config(cls, config_uri = None) :
        
        if not config_uri :
            config_uri = default_config_uri(cls.workspace)
        
        # persist in remote and local storage
        full_path = f"{cls.local_dir}/blueprint-config.json"
        cls._write_to_local_storage(full_path)
        cls._write_to_remote_storage(full_path, config_uri)
        
        
    def ws_prefix(self):
        
        return self.config["workspace-config"]["s3_prefix"]

    def sample_data_uri(self) :

        sample_path = self.config["data-config"]["raw_in_prefix"]
        return f"s3://{self.workspace}/{sample_path}"
    
    def update_automl_config(self, automl_config) :
        
        self.config["automl-config"] = automl_config
        self._persist_bp_config()
        