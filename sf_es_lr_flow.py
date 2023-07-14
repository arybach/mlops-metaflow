import pandas as pd
from scipy import stats
from sklearn.metrics import mean_squared_error, r2_score
from elastic import get_elastic_client
from metaflow import FlowSpec, step, card, current, Parameter
from metaflow.cards import Image
from sklearn.linear_model import LinearRegression
import numpy as np
from utils import upload_to_s3
import matplotlib.pyplot as plt
import joblib
import io, os

class SfEsLrFlow(FlowSpec):
    """
    A Metaflow flow for saving docs from ES nutrients index to s3 bucket
    """
    @card
    @step
    def start(self):
        """Fetch training, validation, and testing docs from Elasticsearch index"""

        self.bucket_name = "mlops-nutrients"
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

        self.next(self.linear_regression)

    @card
    @step
    def linear_regression(self):
        """ Run linear regression model to predict score """

        # Split training DataFrame into X_train and y_train
        X_train = self.training_df.drop(columns=["fid", "description", "score"])
        y_train = self.training_df["score"]

        # Split validation DataFrame into X_val and y_val
        X_val = self.validation_df.drop(columns=["fid", "description", "score"])
        y_val = self.validation_df["score"]
        
        self.X_val = X_val
        self.y_val = y_val

        # Split testing DataFrame into X_test and y_test
        X_test = self.testing_df.drop(columns=["fid", "description", "score"])
        y_test = self.testing_df["score"]

        # Create an instance of the LinearRegression model
        regression = LinearRegression()

        # Fit the model to the training data
        regression.fit(X_train, y_train)

        # Predict the 'score' for the test data
        self.y_test = y_test
        self.y_pred = regression.predict(X_test)

        # Evaluate the model on the validation data
        self.val_score = regression.score(X_val, y_val)
        print("Validation score:", self.val_score)

        # Evaluate the model on the test data
        self.test_score = regression.score(X_test, y_test)
        print("Test score:", self.test_score)

        # Save the trained model as an artifact
        self.regression = regression

        self.next(self.visualize)

    # python jupyter/sf_es_lr_flow.py card view visualize
    # on the left side of the VS code explorer under metaflow_card_cache right click on the card and open in the browser
    @card
    @step
    def visualize(self):
        """Visualize regression residuals and calculate MSE, RMSE, R2 score"""

        # Make predictions on the validation data
        y_pred_val = self.regression.predict(self.X_val.values)

        # Convert y_val to a NumPy array
        y_val = self.y_val.values

        # Calculate residuals
        residuals = y_val - y_pred_val

        # Plot the residuals
        plt.figure()
        plt.hist(residuals, bins=30)
        plt.xlabel('Residuals')
        plt.ylabel('Frequency')
        plt.title('Residual Distribution')
        current.card.append(Image.from_matplotlib(plt.gcf()))

        # Check the normality of residuals using Q-Q plot
        plt.figure()
        stats.probplot(residuals, dist="norm", plot=plt)
        plt.title('Q-Q Plot - Residuals')
        current.card.append(Image.from_matplotlib(plt.gcf()))

        # Calculate and print additional evaluation metrics
        self.mse = mean_squared_error(y_val, y_pred_val)
        self.rmse = np.sqrt(self.mse)
        self.r2 = r2_score(y_val, y_pred_val)
        print("Mean Squared Error (MSE):", self.mse)
        print("Root Mean Squared Error (RMSE):", self.rmse)
        print("R-squared (R2) Score:", self.r2)

        # Scatter plot of predicted vs. actual values
        plt.figure()
        plt.scatter(y_val, y_pred_val)
        plt.xlabel('Actual Values')
        plt.ylabel('Predicted Values')
        plt.title('Scatter Plot - Predicted vs. Actual Values')
        current.card.append(Image.from_matplotlib(plt.gcf()))

        # Residuals vs. Predicted Values plot
        plt.figure()
        plt.scatter(y_pred_val, residuals)
        plt.xlabel('Predicted Values')
        plt.ylabel('Residuals')
        plt.title('Residuals vs. Predicted Values')
        current.card.append(Image.from_matplotlib(plt.gcf()))

        # Residuals vs. Independent Variables plots
        X_val = pd.DataFrame(self.X_val)
        independent_vars = X_val.columns
        for var in independent_vars:
            plt.figure()
            plt.scatter(X_val[var], residuals)
            plt.xlabel(var)
            plt.ylabel('Residuals')
            plt.title(f'Residuals vs. {var}')
            current.card.append(Image.from_matplotlib(plt.gcf()))

        # Histogram of Residuals
        plt.figure()
        plt.hist(residuals, bins=30)
        plt.xlabel('Residuals')
        plt.ylabel('Frequency')
        plt.title('Histogram of Residuals')
        current.card.append(Image.from_matplotlib(plt.gcf()))

        # QQ Plot of Residuals
        plt.figure()
        stats.probplot(residuals, dist="norm", plot=plt)
        plt.title('Q-Q Plot - Residuals')
        current.card.append(Image.from_matplotlib(plt.gcf()))

        self.next(self.end)

    @step
    def end(self):
        """Save the models to s3://{bucket_name}/{models}{model_name.pkl} files """
        # Save the model to a file
        self.regression_path = 'models/linear_regression_model.pkl'

        # Create the directory if it doesn't exist
        os.makedirs('models', exist_ok=True)

        joblib.dump(self.regression, self.regression_path)

        # Upload the files to S3
        self.path_to_regression_model = upload_to_s3(self.regression_path, self.bucket_name)
 

if __name__ == '__main__':
    flow = SfEsLrFlow()
    flow.run()
