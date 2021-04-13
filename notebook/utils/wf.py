from utils.bpconfig import BPConfig

import json
from tqdm.notebook import trange, tqdm
from time import sleep, time
from urllib.parse import urlparse

import pandas as pd

import boto3

class BPRunner :
    
    #Interim code to provide some decoupling of underlying workflow engines
    #######################################################################
    ENGINES = {
        "botocore.client.SFN": {
            "type": "stepfunctions",
        }
    }
     
    @classmethod
    def classname(cls, obj) :
        cls = type(obj)
        module = cls.__module__
        name = cls.__qualname__
        
        if module is not None and module != "__builtin__" :
            name = module + "." + name
            
        return name
    
    @classmethod
    def _configure_sfn_driver(cls, wf_driver) :
        BPRunner.ENGINES["botocore.client.SFN"]["run"] = cls._run_sfn_workflow
        BPRunner.ENGINES["botocore.client.SFN"]["monitor"] = SFNMonitor(client = wf_driver)

    @classmethod
    def _load_config(cls) :
        BPRunner.ENGINES["botocore.client.SFN"]["init"] = cls._configure_sfn_driver
    #######################################################################
    
    def __init__(self, workspace, db_driver=None, wf_driver=None) :
        
        #default StepFunctions as the workflow engine           
        self.client = wf_driver if wf_driver else  boto3.client(BPRunner.ENGINES["sfn"])
        self.db = db_driver if db_driver else boto3.client("s3")
    
        self.client_type = self.classname(self.client)
        if not self.client_type in BPRunner.ENGINES :
            raise Exception(f"Client {self.client_type} is not supported.")
     
        self.workspace = workspace
        self._load_config()
        BPRunner.ENGINES[self.client_type]["init"](wf_driver)
            
    def run_blueprint(self, name, n_stages, wait=False) :
        
        if BPRunner.ENGINES[self.client_type]["type"] == "stepfunctions" :
            wf_id = self.find_sfn_arn(name)
            input_config = {"config_uri": BPConfig.default_config_uri(self.workspace)}
            execution_id = BPRunner.ENGINES[self.client_type]["run"](self, wf_id, input_config)
            
            if wait :
                monitor = BPRunner.ENGINES[self.client_type]["monitor"]
                # TODO: need a generic way to obtain the workflow steps. Currently, the progress bar is
                # hardcoded to 9 steps.
                monitor.run(execution_id, n_stages)
                
        return execution_id
              
    def _run_sfn_workflow(self, wf_id, input_config) :

        execution_arn = self.client.start_execution(
            stateMachineArn= wf_id,
            input= json.dumps(input_config)
        )["executionArn"]
        
        return execution_arn
    
    def find_sfn_arn(self, name) :

        workflows = self.client.list_state_machines()['stateMachines']

        for wf in workflows :
            if wf["name"] == name:
                wf_arn = wf["stateMachineArn"]
                return wf_arn
         
    def get_automl_job_name(self, run_id) :
        
        exec_details = self.client.describe_execution(executionArn=run_id)
        status = exec_details["status"] if "status" in exec_details else "UNKNOWN"
        
        if status != "SUCCEEDED" :
            raise Exception(f"{run_id} must have a SUCCEEDED status. Status is {status}.")     
        
        return json.loads(exec_details["output"])[0]["Payload"]["automl-config"]["job_name"]
    
    def get_best_model_name(self, run_id) :
        
        exec_details = self.client.describe_execution(executionArn=run_id)
        status = exec_details["status"] if "status" in exec_details else "UNKNOWN"
        
        if status != "SUCCEEDED" :
            raise Exception(f"{run_id} must have a SUCCEEDED status. Status is {status}.")     
        
        return json.loads(exec_details["output"])[0]["Payload"]["model-config"]["model_name"]
    
    @classmethod
    def _get_merged_df(cls, bucket, prefix, s3_client, show_header=True, has_header=True, maxkeys=10) :
        
        files = []
        skip = 0
        kwargs = {'Bucket': bucket, 'Prefix': prefix, 'MaxKeys': maxkeys}
        resp = s3_client.list_objects_v2(**kwargs)
        
        for obj in resp['Contents'] :
            
            if has_header and not show_header :
                skip = 1
                
            if has_header and show_header :
                files.append(pd.read_csv("s3://{}/{}".format(bucket, obj["Key"])))
            else :
                files.append(pd.read_csv("s3://{}/{}".format(bucket, obj["Key"]), skiprows=skip, header=None))
                
        df = pd.concat(files)

        return df

    def get_prepped_data_df(self, run_id, has_header=True, maxkeys=10) :
        
        exec_details = self.client.describe_execution(executionArn=run_id)
        status = exec_details["status"] if "status" in exec_details else "UNKNOWN"
        
        if status != "SUCCEEDED" :
            raise Exception(f"{run_id} must have a SUCCEEDED status. Status is {status}.")     
        
        data_uri = json.loads(exec_details["output"])[0]["Payload"]["automl-config"]["data_uri"]   
        
        parsed = urlparse(data_uri, allow_fragments=False)
        if parsed.query:
            prefix= parsed.path.lstrip('/') + '?' + parsed.query
        else:
            prefix= parsed.path.lstrip('/')

        return self._get_merged_df(parsed.netloc, prefix, self.db, has_header, maxkeys)
        
        
class SFNMonitor() :

    PAGINATION_SIZE = 100
    TIMEOUT = 10800
    POLL_RATE = 30

    def __init__(self, timeout=None, n_wait=None, client = None) :
        
        self.timeout = timeout if timeout else SFNMonitor.TIMEOUT
        self.n_wait = n_wait if n_wait else SFNMonitor.POLL_RATE
        self.sfn = boto3.client("stepfunctions") if not client else client
            
    def _update_progress(self, pb, unit=1) :

        pb.update(unit)
        sleep(0.5)
    
    def _wf_failed(self, status) :
        failed_states = {'FAILED','TIMED_OUT','ABORTED'}
        return (status in failed_states)

    def _monitor_parallel_process(self, execution_arn, lookup, parent_pb) :
    
        completed = 0

        start = time()
        while True :

            status = self.sfn.describe_execution(executionArn=execution_arn)["status"]
            if self._wf_failed(status) :
                parent_pb.leave=True
                return;

            events = self.sfn.get_execution_history(
                                    executionArn=execution_arn,
                                    maxResults=200,
                                    reverseOrder=False,
                                    includeExecutionData=False)["events"]

            for e in events :

                if "stateEnteredEventDetails" in e :
                    state = e["stateEnteredEventDetails"]["name"]
                    transition= e["type"]

                    #print(f"state entered: {state} status: {transition}")

                    if state not in lookup :

                        if transition != "ParallelStateEntered" :
                            child_pb = trange(1, desc=f">> Parallel Stage: {state}")
                            lookup[state] = child_pb
                        else :
                            lookup[state] = 1
                            ntasks = self._monitor_parallel_process(execution_arn, lookup, parent_pb)
                            completed += ntasks

                if "stateExitedEventDetails" in e :
                    state = e["stateExitedEventDetails"]["name"]
                    transition = e["type"]

                    #print(f"state exited: {state} status: {transition}")

                    assert state in lookup

                    if lookup[state] :
                        completed+=1
                        self._update_progress(parent_pb)

                        if transition != "ParallelStateExited" :
                            self._update_progress(lookup[state])
                            lookup[state] = 0
                        else :
                            lookup[state] = 0
                            return completed

            if time() - start > self.timeout :
                parent_pb.leave = True
                break;

            sleep(self.n_wait)

        return completed

    def run(self, execution_arn, n_stages) :
        
        lookup= {}
        completed = 0
        main_pb = trange(n_stages+1, desc="Workflow Initiated")

        start = time()
        while True :

            status = self.sfn.describe_execution(executionArn=execution_arn)["status"]
            if self._wf_failed(status) :
                main_pb.leave=True
                raise Exception(f"Workflow execution {status}.")

            exec_hist = self.sfn.get_execution_history(
                                executionArn=execution_arn,
                                maxResults=SFNMonitor.PAGINATION_SIZE,
                                reverseOrder=False,
                                includeExecutionData=False)
            
            events = exec_hist["events"]

            while True :
                
                for e in events :

                    if "stateEnteredEventDetails" in e :
                        state = e["stateEnteredEventDetails"]["name"]
                        transition= e["type"]

                        #print(f"state entered: {state} status: {transition}")

                        if state not in lookup :

                            lookup[state] = 1
                            main_pb.desc = f"Currently in the workflow stage: {state}"

                            if transition == "ParallelStateEntered" :
                                ntasks = self._monitor_parallel_process(execution_arn, lookup, main_pb)
                                completed += ntasks

                    if "stateExitedEventDetails" in e :
                        state = e["stateExitedEventDetails"]["name"]
                        transition = e["type"]

                        #print(f"state exited: {state} status: {transition}")

                        assert (state in lookup)

                        if lookup[state] :

                            if transition != "ParallelStateExited" :
                                self._update_progress(main_pb)
                                lookup[state] = 0
                                completed+=1
                            else :
                                assert not lookup[state]
                                
                if "nextToken" not in exec_hist :
                    break
                
                next_token = exec_hist["nextToken"]
                exec_hist = self.sfn.get_execution_history(
                                    executionArn=execution_arn,
                                    maxResults=SFNMonitor.PAGINATION_SIZE,
                                    reverseOrder=False,
                                    includeExecutionData=False,
                                    nextToken = next_token) 
                                
            if time() - start > self.timeout :
                main_pb.leave = True
                break

            if completed >= n_stages :
                main_pb.desc = f"Workflow Completed"
                self._update_progress(main_pb)
                break
            else :
                sleep(self.n_wait) 