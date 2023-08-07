import requests
import pandas as pd
import logging
import pandas as pd
from metaflow import Flow, FlowSpec, step, card, current, batch, Parameter, retry
#from metaflow.cards import Image
from config import fastapi_host, image
from utils import upload_to_s3, get_evidently_html
from time import sleep

class SfEsEvidentlyFastApi(FlowSpec):

    @card
    @batch(cpu=2, memory=3500,image=image)
    @step
    def start(self):
        """Start the flow and get docs from a prior flow."""

        # Get streams from a prior run
        download_flow_data = Flow("SfEsEvidently")
        self.testing_df = download_flow_data.latest_run["fetch_notlabeled"].task.data.testing_df
        self.notlabeled_df = download_flow_data.latest_run["fetch_notlabeled"].task.data.notlabeled_df

        # download_xgboost_flow = Flow("SfEsXGBoostFlow")
        # self.xgboost_model = download_xgboost_flow.latest_run["xgboost_regression"].task.data.xgboost_model

        # download_regression_flow = Flow("SfEsLrFlow")
        # self.regression_model = download_regression_flow.latest_run["linear_regression"].task.data.regression

        self.next(self.calc_predictions)


    @batch(cpu=2, memory=7500,image=image)
    #@retry(times=2)
    @step
    def calc_predictions(self):
        
        batch_size = 100  # Set the batch size as needed

        reference_xgboost_preds = []
        reference_regression_preds = []
        current_xgboost_preds = []
        current_regression_preds = []

        # Replace with appropriate URL and port of your FastAPI on EC2
        url_xgboost = f'http://{fastapi_host}:5000/predict/xgboost_model'
        url_regression = f'http://{fastapi_host}:5000/predict/linear_regression_model'

        # calculate predictions for reference data
        for i in range(0, len(self.testing_df), batch_size):
            # Get the batch of features
            feature_batch = self.testing_df[i:i + batch_size]
            
            # Make prediction request for xgboost
            resp_xgboost = requests.post(
                url=url_xgboost,
                json={'features': feature_batch.to_json()},
                timeout=20
            )
            resp_regression = requests.post(
                url=url_regression,
                json={'features': feature_batch.to_json()},
                timeout=20
            )

            if resp_xgboost.json():
                preds_json_xgboost = resp_xgboost.json().get('predictions')
            else:
                logging.info(f"Missing xgboost predictions: {resp_xgboost.json()}")
                #logging.error(f"Request: {df.to_json()}")

            if resp_regression.json():
                preds_json_regression = resp_regression.json().get('predictions')
            else:
                logging.info(f"Missing regression predictions: {resp_regression.json()}")
                #logging.error(f"Request: {df.to_json()}")

            # Convert JSON predictions to DataFrame and select necessary columns
            if preds_json_xgboost:
                predictions_xgboost = pd.read_json(preds_json_xgboost)
            else:
                logging.info(f"Missing xgboost predictions: {preds_json_xgboost}")
            
            if preds_json_regression:
                predictions_regression = pd.read_json(preds_json_regression)
            else:
                logging.info(f"Missing regression predictions: {preds_json_regression}")

            # Add predictions to corresponding DataFrame
            reference_xgboost_preds.extend(predictions_xgboost)
            reference_regression_preds.extend(predictions_regression)

        # calculate predictions for reference data
        for i in range(0, len(self.notlabeled_df), batch_size):
            # Get the batch of features
            feature_batch = self.notlabeled_df[i:i + batch_size]

            # Make prediction request for xgboost
            resp_xgboost = requests.post(
                url=url_xgboost,
                json={'features': feature_batch.to_json()},
                timeout=20
            )
            resp_regression = requests.post(
                url=url_regression,
                json={'features': feature_batch.to_json()},
                timeout=20
            )

            if resp_xgboost.json():
                preds_json_xgboost = resp_xgboost.json().get('predictions')
            else:
                logging.info(f"Missing xgboost predictions: {resp_xgboost.json()}")
                #logging.error(f"Request: {df.to_json()}")

            if resp_regression.json():
                preds_json_regression = resp_regression.json().get('predictions')
            else:
                logging.info(f"Missing regression predictions: {resp_regression.json()}")
                #logging.error(f"Request: {df.to_json()}")

            # Convert JSON predictions to DataFrame and select necessary columns
            if preds_json_xgboost:
                predictions_xgboost = pd.read_json(preds_json_xgboost)
            else:
                logging.info(f"Missing xgboost predictions: {preds_json_xgboost}")
            
            if preds_json_regression:
                predictions_regression = pd.read_json(preds_json_regression)
            else:
                logging.info(f"Missing regression predictions: {preds_json_regression}")

            # Add predictions to corresponding DataFrame
            current_xgboost_preds.extend(predictions_xgboost)
            current_regression_preds.extend(predictions_regression)

        self.testing_df['xgboost_predictions'] = pd.Series(reference_xgboost_preds)
        self.testing_df['regression_predictions'] = pd.Series(reference_regression_preds)
        self.notlabeled_df['xgboost_predictions'] = pd.Series(current_xgboost_preds)
        self.notlabeled_df['regression_predictions'] = pd.Series(current_regression_preds)
        self.next(self.monitoring_data_quality)


    @card(type='html',options={"attribute":"data_quality"})
    @batch(cpu=2, memory=7500,image=image)
    #@retry(times=2)
    @step
    def monitoring_data_quality(self):
        import os

        os.system("pip install evidently --quiet")
        from evidently.test_preset import DataStabilityTestPreset
        from evidently.test_suite import TestSuite
        from evidently.report import Report
        from evidently.metric_preset.data_quality import DataQualityPreset

        print("Monitoring: data quality tests")
        data_stability = TestSuite(
            tests=[
                DataStabilityTestPreset(),
            ]
        )
        # drift testing new data using testing data as reference (to have a diff sample when currently is in the fastapi db)
        data_stability.run(reference_data=self.testing_df.drop(["xgboost_predictions", "regression_predictions"], axis=1), 
                           current_data=self.notlabeled_df.drop(["xgboost_predictions", "regression_predictions"], axis=1))
        # save as an artifact
        self.data_stability = pd.json_normalize(data_stability.as_dict())
        
        data_quality_report = Report(metrics=[
            DataQualityPreset(),
        ])
        
        data_quality_report.run(reference_data=self.testing_df.drop(["xgboost_predictions", "regression_predictions"], axis=1), 
                           current_data=self.notlabeled_df.drop(["xgboost_predictions", "regression_predictions"], axis=1))
        # save as html to be rendered by metaflow card
        self.data_quality = get_evidently_html(data_quality_report)

        self.next(self.data_drift_test)


    @card(type='html',options={"attribute":"drift_report"})
    @batch(cpu=2, memory=7500,image=image)
    @step
    def data_drift_test(self):
        import os

        os.system("pip install evidently --quiet")
        from evidently.test_preset import DataDriftTestPreset
        from evidently.test_suite import TestSuite
        from evidently import ColumnMapping
        from evidently.report import Report
        from evidently.metric_preset.data_drift import DataDriftPreset

        column_mapping = ColumnMapping()

        column_mapping.target = 'score' #'y' is the name of the column with the target function
        # column_mapping.prediction = 'pred' #'pred' is the name of the column(s) with model predictions
        column_mapping.id = 'fid' #there is no ID column in the dataset
        column_mapping.embeddings = None #there is no embeddings column in the dataset

        print("Drift Test: comparing data distributions")

        columns = [ clmn for clmn in self.testing_df.columns if clmn not in [ 'fid', 'description', 'score', 'xgboost_predictions', 'regression_predictions' ]]

        # Instantiate the DataDriftTestPreset with the desired parameters
        drift_preset = DataDriftTestPreset(
            columns=columns,  # List of columns to include in the drift analysis
            drift_share=0.5,  # Specify the drift share
            stattest="ks",  # Specify the statistical test for drift detection, e.g., "ks" for Kolmogorov-Smirnov test
            # You can specify additional parameters as needed for your drift analysis
            # For example: cat_stattest, num_stattest, text_stattest, etc.
        )

        # Generate the drift metrics using the DataDriftTestPreset
        drift_metrics = TestSuite(tests=[drift_preset])
        drift_metrics.run(reference_data=self.testing_df.drop(["xgboost_predictions", "regression_predictions"], axis=1), 
                           current_data=self.notlabeled_df.drop(["xgboost_predictions", "regression_predictions"], axis=1),
                          column_mapping=column_mapping)

        self.drift_metrics = pd.json_normalize(drift_metrics.as_dict())

        data_drift_report = Report(metrics=[
            DataDriftPreset(),
        ])
        data_drift_report.run(reference_data=self.testing_df.drop(["xgboost_predictions", "regression_predictions"], axis=1), 
                           current_data=self.notlabeled_df.drop(["xgboost_predictions", "regression_predictions"], axis=1))
        # save as html artifact to be rendered by metaflow card view
        self.drift_report = get_evidently_html(data_drift_report)
        
        self.next(self.regression_model_performance)


    @card(type='html',options={"attribute":"model_performance"})
    @batch(cpu=2, memory=7500,image=image)
    #@retry(times=2)
    @step
    def regression_model_performance(self):
        # fetches report from the fastapi endpoint
        model_name = 'linear_regression_model'
        window_size = 3000  # Set the window size as needed

        # URL of your FastAPI on EC2 for monitoring model performance
        url = f'http://{fastapi_host}:5000/monitor-model/{model_name}?window_size={window_size}'

        try:
            # Make the GET request
            resp = requests.get(url=url)

            if resp.status_code == 200:
                print(resp.text)
                # download html file
                # html_file = f'http://{fastapi_host}:5000/download-file/{resp}'
   
                # Use the function to save the HTML as an artifact
                # self.model_performance = requests.get(html_file)
                self.model_performance = resp.content.decode('utf-8')
                #self.model_performance = resp.text

            print(f"Report for {model_name} is saved.")

        except Exception as e:
            print(f"Failed to generate report for {model_name}: {e}")

        self.next(self.xgboost_model_performance)


    @card(type='html',options={"attribute":"model_performance"})
    @batch(cpu=2, memory=7500,image=image)
    #@retry(times=2)
    @step
    def xgboost_model_performance(self):
        # fetches report from the fastapi endpoint
        model_name = 'xgboost_model'
        window_size = 3000  # Set the window size as needed

        # URL of your FastAPI on EC2 for monitoring model performance
        url = f'http://{fastapi_host}:5000/monitor-model/{model_name}?window_size={window_size}'

        try:
            # Make the GET request
            resp = requests.get(url=url,timeout=120)

            if resp.status_code == 200:
                print(resp.text)
                # download html file
                # html_file = f'http://{fastapi_host}:5000/download-file/{resp}'
   
                # Use the function to save the HTML as an artifact
                # self.model_performance = requests.get(html_file)
                self.model_performance = resp.content.decode('utf-8')

            print(f"Report for {model_name} is saved.")

        except Exception as e:
            print(f"Failed to generate report for {model_name}: {e}")

        self.next(self.regression_target_monitoring)


    @card(type='html',options={"attribute":"regression_target"})
    @batch(cpu=2, memory=7500,image=image)
    #@retry(times=2)
    @step
    def regression_target_monitoring(self):
        # fetches report from the fastapi endpoint
        model_name = 'linear_regression_model'
        window_size = 3000  # Set the window size as needed

        # URL of your FastAPI on EC2 for monitoring model performance
        url = f'http://{fastapi_host}:5000/monitor-target/{model_name}?window_size={window_size}'

        try:
            # Make the GET request
            resp = requests.get(url=url,timeout=120)

            if resp.status_code == 200:
                self.regression_target = resp.text

            print(f"Report for {model_name} target and predictions is saved.")

        except Exception as e:
            print(f"Failed to generate report for {model_name} target and predictions: {e}")

        self.next(self.xgboost_target_monitoring)


    @card(type='html',options={"attribute":"xgboost_target"})
    @batch(cpu=2, memory=7500,image=image)
    #@retry(times=2)
    @step
    def xgboost_target_monitoring(self):
        # fetches report from the fastapi endpoint
        model_name = 'xgboost_model'
        window_size = 3000  # Set the window size as needed

        # URL of your FastAPI on EC2 for monitoring model performance
        url = f'http://{fastapi_host}:5000/monitor-target/{model_name}?window_size={window_size}'

        try:
            # Make the GET request
            resp = requests.get(url=url,timeout=120)

            if resp.status_code == 200:
                self.xgboost_target = resp.text

            print(f"Report for {model_name} target and predictions is saved.")

        except Exception as e:
            print(f"Failed to generate report for {model_name} target and predictions: {e}")

        self.next(self.end)

    @batch(cpu=2, memory=3500,image=image)
    @step
    def end(self):
        print("Drift test completed")

if __name__ == '__main__':
    flow = SfEsEvidentlyFastApi()
    flow.run()
