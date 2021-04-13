import json
import os

import matplotlib.pyplot as plt
import ipywidgets as widgets
from ipywidgets import interact, interactive, fixed, interact_manual
import seaborn as sns

import pandas as pd
import numpy as np
import shap
from sklearn.metrics import roc_curve, auc, RocCurveDisplay, confusion_matrix

import boto3

from sagemaker.s3 import S3Downloader
        
class ModelInspector() :

    def __init__(self, config) :
        
        self.bucket = config["workspace"]
        self.results_prefix = config["prefixes"]["results_path"]
        self.bias_prefix = config["prefixes"]["bias_path"]
        self.xai_prefix = config["prefixes"]["xai_path"]
        
        self.gt_idx = config["results-config"]["gt_index"]
        self.pred_idx = config["results-config"]["pred_index"]

        self.s3 = boto3.client("s3")        
        
        self.results_df =self._get_merged_df(self.bucket, self.results_prefix)
        
    def get_results(self) :
        return self.results_df
    
    def _get_merged_df(self, bucket, prefix, has_header=True, maxkeys=10) :
        
        files = []
        skip = 0
        kwargs = {'Bucket': bucket, 'Prefix': prefix, 'MaxKeys': maxkeys}
        resp = self.s3.list_objects_v2(**kwargs)
        for obj in resp['Contents'] :
            
            if (has_header) :
                skip = 1

            files.append(pd.read_csv("s3://{}/{}".format(bucket, obj["Key"]), skiprows=skip, header=None))
                
        df = pd.concat(files)

        return df
    
    def get_roc_curve(self, gt_index=0, pred_index=1, display=True, model_name="autopilot-model") :
            
        y = self._y()
        yh = self._yh()
        
        fpr, tpr, thresholds = roc_curve(y, yh)
        roc_auc = auc(fpr, tpr)

        viz = RocCurveDisplay(fpr=fpr, tpr=tpr, roc_auc=roc_auc, estimator_name=model_name) 

        if display :
            viz.plot()
            
        return viz, roc_auc, fpr, tpr, thresholds
        

    def visualize_auc(self, fpr, tpr, thresholds) :
        
        df = pd.DataFrame({
            "False Positive Rate":fpr,
            "True Positive Rate":tpr,
            "Threshold":thresholds
        })

        axes = df.plot.area(stacked=False, x="Threshold", figsize=(20,3),colormap='RdGy', alpha=0.3)
        axes.set_xlabel("Threshold")
        axes.set_ylabel("Rate")
        axes.set_xlim(0,1.0)
        axes.set_ylim(0,1.0)
        
    def _y(self) :
        return self.results_df[self.gt_idx]
    
    def _yh(self) :
        return self.results_df[self.pred_idx]
    
    def display_interactive_cm(self, start=0.5, min=0.0, max=1.0, step=0.05) :

        y = self._y()
        yh = self._yh()
        
        def cm_heatmap_fn(Threshold) :

            cm = confusion_matrix(y, yh >= Threshold).astype(int)

            names = ['True Neg','False Pos','False Neg','True Pos']
            counts = ["{0:0.0f}".format(value)
                            for value in cm.flatten()]

            pcts = ["{0:.2%}".format(value)
                                 for value in cm.flatten()/np.sum(cm)]

            labels = [f"{v1}\n{v2}\n{v3}"
                      for v1, v2, v3 in zip(names,counts,pcts)]

            labels = np.asarray(labels).reshape(2,2)
            sns.heatmap(cm, annot=labels, fmt='', cmap='Blues')

        thresh_slider = widgets.FloatSlider(value=start,
                                            min=min,
                                            max=max,
                                            step=step)

        interact(cm_heatmap_fn, Threshold=thresh_slider)
        
    def _download_clarify_xai_summary(self) :
        
        try :
    
            summary_uri = f"s3://{self.bucket}/{self.xai_prefix}/analysis.json"
            S3Downloader.download(summary_uri, os.getcwd())

            with open('analysis.json', 'r') as f:
                summary = json.loads(f.read())

            return summary
    
        except Exception as e:
            print(f"{e}: Failed to download {xai_summary}")
            
    
    def explain_prediction(self, data_row_id) :
    
        xai_summary = self._download_clarify_xai_summary()
        
        columns = list(xai_summary['explanations']['kernel_shap']['label0']["global_shap_values"].keys())
        xai_results = f"s3://{self.bucket}/{self.xai_prefix}/explanations_shap/out.csv"
        shap_df = pd.read_csv(xai_results)

        y = self._y()
        yh = self._yh()
        
        descr = "Yes, this client opened a term deposit. " if (y.iloc[data_row_id]) else "No, this client did not open a term deposit. "
        descr+= "The model predicts that the probability that this prospect will open a deposit is {:3f}. \n".format(yh.iloc[data_row_id])
        print(descr)
    
        expected_value = xai_summary['explanations']['kernel_shap']['label0']['expected_value']
        shap.force_plot(expected_value, np.array(shap_df.iloc[data_row_id,:]), np.array(columns), matplotlib=True)