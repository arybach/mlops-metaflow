import requests
import pandas as pd
import logging
import pandas as pd
from metaflow import Flow, FlowSpec, step, card, current, batch, Parameter, retry
#from metaflow.cards import Image
from config import fastapi_host, image, bucket_name, index
from utils import upload_to_s3, get_evidently_html
from elastic import get_elastic_client
from time import sleep

class SfEsEvidentlyFastApi(FlowSpec):

    @batch(cpu=2, memory=3500,image=image)
    @card
    @step
    def start(self):
        """Fetch training, validation, and testing docs from Elasticsearch index"""

        self.bucket_name = bucket_name
        # Connect to Elasticsearch
        es = get_elastic_client("local")  # Update with your Elasticsearch configuration

        # Define the index name
        self.index_name = "labeled"
        scroll_size = 10000  # Adjust the scroll size based on your requirements

        # Define the labels and corresponding datasets
        labels = ["training", "validation", "testing"]

        for label in labels:
            # Define the query to retrieve documents for the current label
            query = {
                "query": {
                    "term": {
                        "label.keyword": label
                    }
                },
                "size": scroll_size
            }
            # Initial request
            hits = []
            response = es.search(index=self.index_name, body=query, scroll="2m") # pylint: disable=E1123
            scroll_id = response["_scroll_id"]
            hits.extend(response["hits"]["hits"])

            # Subsequent requests
            while True:
                response = es.scroll(scroll_id=scroll_id, scroll="2m")
                scroll_id = response["_scroll_id"]
                res = response["hits"]["hits"]
                if not res:
                    break
                hits.extend(res)

            data = []
            for hit in hits:
                source = hit["_source"]
                if source and source["labelNutrients"]:
                    # using declared nutrition values
                    # Fill missing values with 0 (as it makes sense in this case)
                    fields = [ "fat", "saturatedFat", "transFat", "cholesterol", "sodium", "carbohydrates", "fiber", "sugars", 
                              "protein", "calcium", "iron", "potassium", "calories" ]

                    item = {
                        "fid": source["fid"],
                        "description": source["description"],
                        "score": source["score"]
                    }
                    for field in fields:
                        if source and source["labelNutrients"]:
                            amount = source["labelNutrients"].get(field, {"amount": 0})
                            if amount:
                                val = amount.get("amount", 0)
                                if val:
                                    item.update({field: val})
                                else:
                                    item.update({field: 0})
                            else:
                                amount = {"amount": 0}
                                val = amount.get("amount", 0)
                                if val:
                                    item.update({field: val})
                                else:
                                    item.update({field: 0})

                    data.append(item)

                elif source and source["nutrients"]:
                    # using calculated nutrition values
                    # Fill missing values with 0 (as it makes sense in this case)
                    fields = [ "fat", "saturatedFat", "transFat", "cholesterol", "sodium", "carbohydrates", "fiber", "sugars", 
                              "protein", "calcium", "iron", "potassium", "calories" ]

                    item = {
                        "fid": source["fid"],
                        "description": source["description"],
                        "score": source["score"]
                    }
                    for field in fields:
                        if source and source["nutrients"]:
                            amount = source["nutrients"].get(field, {"amount": 0})
                            if amount:
                                val = amount.get("amount", 0)
                                if val:
                                    item.update({field: val})
                                else:
                                    item.update({field: 0})
                            else:
                                amount = {"amount": 0}
                                val = amount.get("amount", 0)
                                if val:
                                    item.update({field: val})
                                else:
                                    item.update({field: 0})

                    data.append(item)

            # Convert the documents to a pandas DataFrame and assign it to the corresponding dataset
            if label == 'training':
                self.training_df = pd.DataFrame(data)
            elif label == 'validation':
                self.validation_df = pd.DataFrame(data)
            elif label == 'testing':
                self.testing_df = pd.DataFrame(data)

        self.next(self.fetch_notlabeled)

    @batch(cpu=2, memory=3500,image=image)
    @card
    @step
    def fetch_notlabeled(self):
        """Start the flow and get all docs from nutrients index which are not in labeled index."""

        self.bucket_name = bucket_name
        # Connect to Elasticsearch
        es = get_elastic_client("local")  # Update with your Elasticsearch configuration

        # fetch all docs from 
        self.index_name = index
        scroll_size = 10000  # Adjust the scroll size based on your requirements

        # Define the query to retrieve all documents with non-empty nutrients or labelNutrients
        query = {
            "query": {
                "bool": {
                    "should": [
                        {"exists": {"field": "nutrients"}},
                        {"exists": {"field": "labelNutrients"}}
                    ]
                }
            },
            "size": scroll_size
        }
        # Initial request
        response = es.search(index=self.index_name, body=query, scroll="2m") # pylint: disable=E1123
        scroll_id = response["_scroll_id"]
        hits = response["hits"]["hits"]

        # Subsequent requests
        while True:
            response = es.scroll(scroll_id=scroll_id, scroll="2m")
            scroll_id = response["_scroll_id"]
            res = response["hits"]["hits"]
            if not res:
                break
            hits.extend(res)

        data = []
        for hit in hits:
            source = hit["_source"]
            if source and source["labelNutrients"]:
                # using declared nutrition values
                # Fill missing values with 0 (as it makes sense in this case)
                fields = [ "fat", "saturatedFat", "transFat", "cholesterol", "sodium", "carbohydrates", "fiber", "sugars", 
                            "protein", "calcium", "iron", "potassium", "calories" ]

                item = {
                    "fid": source["fid"],
                    "description": source["description"],
                    "score": source["score"]
                }
                for field in fields:
                    if source and source["labelNutrients"]:
                        amount = source["labelNutrients"].get(field, {"amount": 0})
                        if amount:
                            val = amount.get("amount", 0)
                            if val:
                                item.update({field: val})
                            else:
                                item.update({field: 0})
                        else:
                            amount = {"amount": 0}
                            val = amount.get("amount", 0)
                            if val:
                                item.update({field: val})
                            else:
                                item.update({field: 0})

                data.append(item)

            elif source and source["nutrients"]:
                # using calculated nutrition values
                # Fill missing values with 0 (as it makes sense in this case)
                fields = [ "fat", "saturatedFat", "transFat", "cholesterol", "sodium", "carbohydrates", "fiber", "sugars", 
                            "protein", "calcium", "iron", "potassium", "calories" ]

                item = {
                    "fid": source["fid"],
                    "description": source["description"],
                    "score": source["score"]
                }
                for field in fields:
                    if source and source["nutrients"]:
                        amount = source["nutrients"].get(field, {"amount": 0})
                        if amount:
                            val = amount.get("amount", 0)
                            if val:
                                item.update({field: val})
                            else:
                                item.update({field: 0})
                        else:
                            amount = {"amount": 0}
                            val = amount.get("amount", 0)
                            if val:
                                item.update({field: val})
                            else:
                                item.update({field: 0})

                data.append(item)

        # Convert the documents to a pandas DataFrame
        print("Nutrients DataFrame:")
        print(pd.DataFrame(data).head())

        nutrients_df = pd.DataFrame(data)
        if 'embedding' in nutrients_df.columns:
            nutrients_df.drop(columns=['embedding'], inplace=True)

        # Print the first few rows of labeled DataFrames before concatenation
        print("Training DataFrame:")
        print(self.training_df.head())

        print("Validation DataFrame:")
        print(self.validation_df.head())

        print("Testing DataFrame:")
        print(self.testing_df.head())

        # Concatenate the dataframes
        labeled_df = pd.concat([self.training_df, self.validation_df, self.testing_df], ignore_index=True)

        # Find the documents that are in nutrients_df but not in labeled_df based on a unique identifier, for example, "doc_id"
        self.notlabeled_df = nutrients_df[~nutrients_df["fid"].isin(labeled_df["fid"])]

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


    @batch(cpu=2, memory=15000,image=image)
    #@retry(times=2)
    @card(type='html',options={"attribute":"data_quality"})
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


    @batch(cpu=2, memory=7500,image=image)
    @card(type='html',options={"attribute":"drift_report"})
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


    @batch(cpu=2, memory=7500,image=image)
    #@retry(times=2)
    @card(type='html',options={"attribute":"model_performance"})
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
                #print(resp.text)
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


    @batch(cpu=2, memory=7500,image=image)
    #@retry(times=2)
    @card(type='html',options={"attribute":"model_performance"})
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
                #print(resp.text)
                # download html file
                # html_file = f'http://{fastapi_host}:5000/download-file/{resp}'
   
                # Use the function to save the HTML as an artifact
                # self.model_performance = requests.get(html_file)
                self.model_performance = resp.content.decode('utf-8')

            print(f"Report for {model_name} is saved.")

        except Exception as e:
            print(f"Failed to generate report for {model_name}: {e}")

        self.next(self.regression_target_monitoring)


    @batch(cpu=2, memory=7500,image=image)
    #@retry(times=2)
    @card(type='html',options={"attribute":"regression_target"})
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


    @batch(cpu=2, memory=7500,image=image)
    #@retry(times=2)
    @card(type='html',options={"attribute":"xgboost_target"})
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

    @batch(cpu=2, memory=3500)
    @step
    def end(self):
        print("Drift test completed")

if __name__ == '__main__':
    flow = SfEsEvidentlyFastApi()
    flow.run()
