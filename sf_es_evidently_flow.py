from pathlib import Path
import boto3
from joblib import load
import pandas as pd
from metaflow import Flow, FlowSpec, step, card, environment, batch, Parameter, retry
#from metaflow.cards import Image
from config import bucket_name, index, image
from utils import upload_to_s3, get_evidently_html
from elastic import get_elastic_client


class SfEsEvidently(FlowSpec):

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

    @batch(cpu=3, memory=15000,image=image)
    #@retry(times=2)
    @step
    def calc_predictions(self):
        """Fetch models from S3 and calculate predictions."""
        s3_client = boto3.client('s3')

        # List the model files in S3
        model_names = [ 'xgboost_model', 'linear_regression_model' ]
        model_files = [ f"{model_name}.joblib" for model_name in model_names ]

        # Create the 'models' directory if it doesn't exist
        models_dir = Path("models")
        models_dir.mkdir(exist_ok=True)

        # Fetch models from S3 and save them in the 'models' directory
        for model_name, model_file in zip(model_names, model_files):
            s3_object_key = f"models/{model_file}"
            local_model_path = models_dir / model_file
            s3_client.download_file(bucket_name, s3_object_key, str(local_model_path))
            print(f"Model '{model_file}' downloaded from S3 and saved in 'models' directory.")

            # Load the model
            model = load(local_model_path)

            # Assign the model to the class attribute
            if model_name == 'xgboost_model':
                self.xgboost_model = model
            elif model_name == 'linear_regression_model':
                self.regression_model = model

        drop_columns = ['fid','description', 'score']

        # Validation dataset
        validation_df = self.validation_df.drop(columns=drop_columns)
        self.validation_df['xgboost_predictions'] = self.xgboost_model.predict(validation_df)
        self.validation_df['regression_predictions'] = self.regression_model.predict(validation_df)

        # Testing dataset
        testing_df = self.testing_df.drop(columns=drop_columns)
        self.testing_df['xgboost_predictions'] = self.xgboost_model.predict(testing_df)
        self.testing_df['regression_predictions'] = self.regression_model.predict(testing_df)

        # Not-labeled dataset
        notlabeled_df = self.notlabeled_df.drop(columns=drop_columns)
        self.notlabeled_df['xgboost_predictions'] = self.xgboost_model.predict(notlabeled_df)
        self.notlabeled_df['regression_predictions'] = self.regression_model.predict(notlabeled_df)

        self.next(self.monitoring_data_quality)


    @batch(cpu=3, memory=18000,image=image)
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
        # Replace self.reference and self.current with appropriate DataFrames
        # for drift testing, such as self.training_df, self.validation_df, etc.
        reference_data = self.validation_df.drop(["xgboost_predictions", "regression_predictions"], axis=1)
        current_data = self.testing_df.drop(["xgboost_predictions", "regression_predictions"], axis=1)

        data_stability.run(reference_data=reference_data, current_data=current_data)
        # save as an artifact
        self.data_stability = pd.json_normalize(data_stability.as_dict())
        
        data_quality_report = Report(metrics=[
            DataQualityPreset(),
        ])
        
        data_quality_report.run(reference_data=reference_data, current_data=current_data)
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

        columns = [ clmn for clmn in self.validation_df.columns if clmn not in [ 'fid', 'description', 'score', 'xgboost_predictions', 'regression_predictions' ]]

        # Instantiate the DataDriftTestPreset with the desired parameters
        drift_preset = DataDriftTestPreset(
            columns=columns,  # List of columns to include in the drift analysis
            drift_share=0.5,  # Specify the drift share
            stattest="ks",  # Specify the statistical test for drift detection, e.g., "ks" for Kolmogorov-Smirnov test
            # You can specify additional parameters as needed for your drift analysis
            # For example: cat_stattest, num_stattest, text_stattest, etc.
        )
        reference_data = self.validation_df.drop(["xgboost_predictions", "regression_predictions"], axis=1)
        current_data = self.testing_df.drop(["xgboost_predictions", "regression_predictions"], axis=1)

        # Generate the drift metrics using the DataDriftTestPreset
        drift_metrics = TestSuite(tests=[drift_preset])
        drift_metrics.run(reference_data=reference_data, current_data=current_data, column_mapping=column_mapping)

        self.drift_metrics = pd.json_normalize(drift_metrics.as_dict())

        data_drift_report = Report(metrics=[
            DataDriftPreset(),
        ])
        data_drift_report.run(reference_data=reference_data, current_data=current_data)
        # save as html artifact to be rendered by metaflow card view
        self.drift_report = get_evidently_html(data_drift_report)
        
        self.next(self.regression_model_performance)


    @batch(cpu=3, memory=15000,image=image)
    #@retry(times=2)
    @card(type='html',options={"attribute":"model_performance"})
    @step
    def regression_model_performance(self):
        import os
        from config import DATA_COLUMNS

        os.system("pip install evidently --quiet")
        from evidently import ColumnMapping
        from evidently.report import Report
        from evidently.metrics import (
            RegressionQualityMetric,
            RegressionPredictedVsActualScatter,
            RegressionPredictedVsActualPlot,
            RegressionErrorPlot,
            RegressionAbsPercentageErrorPlot,
            RegressionErrorDistribution,
            RegressionErrorNormality,
            RegressionTopErrorMetric
        )
        # remove extras, set proper 'predictons' column
        validation_df = self.validation_df.drop(['xgboost_predictions'], axis=1).rename(columns={'regression_predictions': 'predictions'})
        testing_df = self.testing_df.drop(['xgboost_predictions'], axis=1).rename(columns={'regression_predictions': 'predictions'})

        def get_column_mapping(**kwargs) -> ColumnMapping:

            column_mapping = ColumnMapping()
            column_mapping.target = kwargs['target_col']
            column_mapping.prediction = kwargs['prediction_col']
            column_mapping.numerical_features = kwargs['num_features']
            # Check if cat_features is empty
            if kwargs['cat_features']:
                column_mapping.categorical_features = kwargs['cat_features']
            else:
                column_mapping.categorical_features = None

            return column_mapping


        print("Monitoring: model performance tests")
        column_mapping: ColumnMapping = get_column_mapping(**DATA_COLUMNS)
   
        model_performance_report = Report(metrics=[
            RegressionQualityMetric(),
            RegressionPredictedVsActualScatter(),
            RegressionPredictedVsActualPlot(),
            RegressionErrorPlot(),
            RegressionAbsPercentageErrorPlot(),
            RegressionErrorDistribution(),
            RegressionErrorNormality(),
            RegressionTopErrorMetric()
        ])
        model_performance_report.run(
            reference_data=validation_df,
            current_data=testing_df,
            column_mapping=column_mapping
        )
        # save as html to be rendered by metaflow card
        self.model_performance = get_evidently_html(model_performance_report)

        self.next(self.xgboost_model_performance)


    @card(type='html',options={"attribute":"model_performance"})
    @batch(cpu=3, memory=15000,image=image)
    #@retry(times=2)
    @step
    def xgboost_model_performance(self):
        import os
        from config import DATA_COLUMNS

        os.system("pip install evidently --quiet")
        from evidently import ColumnMapping
        from evidently.report import Report
        from evidently.metrics import (
            RegressionQualityMetric,
            RegressionPredictedVsActualScatter,
            RegressionPredictedVsActualPlot,
            RegressionErrorPlot,
            RegressionAbsPercentageErrorPlot,
            RegressionErrorDistribution,
            RegressionErrorNormality,
            RegressionTopErrorMetric
        )
        # remove extras, set proper 'predictons' column
        validation_df = self.validation_df.drop(['regression_predictions'], axis=1).rename(columns={'xgboost_predictions': 'predictions'})
        testing_df = self.testing_df.drop(['regression_predictions'], axis=1).rename(columns={'xgboost_predictions': 'predictions'})

        def get_column_mapping(**kwargs) -> ColumnMapping:

            column_mapping = ColumnMapping()
            column_mapping.target = kwargs['target_col']
            column_mapping.prediction = kwargs['prediction_col']
            column_mapping.numerical_features = kwargs['num_features']
            # Check if cat_features is empty
            if kwargs['cat_features']:
                column_mapping.categorical_features = kwargs['cat_features']
            else:
                column_mapping.categorical_features = None

            return column_mapping


        print("Monitoring: model performance tests")
        column_mapping: ColumnMapping = get_column_mapping(**DATA_COLUMNS)
   
        model_performance_report = Report(metrics=[
            RegressionQualityMetric(),
            RegressionPredictedVsActualScatter(),
            RegressionPredictedVsActualPlot(),
            RegressionErrorPlot(),
            RegressionAbsPercentageErrorPlot(),
            RegressionErrorDistribution(),
            RegressionErrorNormality(),
            RegressionTopErrorMetric()
        ])
        model_performance_report.run(
            reference_data=validation_df,
            current_data=testing_df,
            column_mapping=column_mapping
        )
        # save as html to be rendered by metaflow card
        self.model_performance = get_evidently_html(model_performance_report)

        self.next(self.end)

    #@card
    @batch(cpu=1, memory=3500)
    @step
    def end(self):
        print("Drift test completed")


if __name__ == '__main__':
    flow = SfEsEvidently()
    flow.run()
