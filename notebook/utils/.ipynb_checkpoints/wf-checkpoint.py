import utils.bpconfig

import json
from tqdm.notebook import trange, tqdm
from time import sleep, time

import boto3

WORKFLOW_STAGES = 9
TIMEOUT = 10800
POLL_RATE = 30

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
    def _configure_sfn_driver(cls, py_driver) :
        BPRunner.ENGINES["botocore.client.SFN"]["run"] = cls._run_sfn_workflow
        BPRunner.ENGINES["botocore.client.SFN"]["monitor"] = SFNMonitor(client = py_driver)

    
    @classmethod
    def _load_config(cls) :
        BPRunner.ENGINES["botocore.client.SFN"]["init"] = cls._configure_sfn_driver
        
    
    #######################################################################
    
    def __init__(self, workspace, py_driver=None) :
        
        #default StepFunctions as the workflow engine
        if not py_driver :
            self.client = boto3.client(BPRunner.ENGINES["sfn"])
        else :
            self.client = py_driver
     
        self.client_type = self.classname(self.client)
        if not self.client_type in BPRunner.ENGINES :
            raise Exception(f"Client {self.client_type} is not supported.")
     
        self.workspace = workspace
        self._load_config()
        BPRunner.ENGINES[self.client_type]["init"](py_driver)
            
    def run_blueprint(self, name, wait=False) :
        
        if BPRunner.ENGINES[self.client_type]["type"] == "stepfunctions" :
            wf_id = self.find_sfn_arn(name)
            input_config = {"config_uri": utils.bpconfig.default_config_uri(self.workspace)}
            execution_id = BPRunner.ENGINES[self.client_type]["run"](self, wf_id, input_config)
            
            if wait :
                monitor = BPRunner.ENGINES[self.client_type]["monitor"]
                # TODO: need a generic way to obtain the workflow steps. Currently, the progress bar is
                # hardcoded to 9 steps.
                monitor.run(execution_id)
                
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
        
class SFNMonitor() :

    def __init__(self, timeout=TIMEOUT, n_wait=POLL_RATE, client = None) :
        
        self.timeout = timeout
        self.n_wait = n_wait
        
        if not client :
            self.sfn = boto3.client("stepfunctions")
        else:
            self.sfn = client
        
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

    def run(self, execution_arn, n_stages=WORKFLOW_STAGES) :
        
        lookup= {}
        completed = 0
        main_pb = trange(n_stages+1, desc="Workflow Initiated")

        start = time()
        while True :

            status = self.sfn.describe_execution(executionArn=execution_arn)["status"]
            if self._wf_failed(status) :
                main_pb.leave=True
                raise Exception(f"Workflow execution {status}.")

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

            if time() - start > self.timeout :
                main_pb.leave = True
                break;

            if completed >= n_stages :
                main_pb.desc = f"Workflow Completed"
                self._update_progress(main_pb)
                break;
            else :
                sleep(self.n_wait) 