import pandas as pd
import xgboost as xgb
from scipy import stats
from sklearn.metrics import mean_squared_error, r2_score
from elastic import get_elastic_client
from metaflow import FlowSpec, step, card, batch, current, Parameter
from metaflow.cards import Image
from config import bucket_name, image
import numpy as np
from utils import upload_to_s3
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
import joblib
import io, os

class SfEsXGBoostFlow(FlowSpec):
    """
    A Metaflow flow for saving docs from ES nutrients index to s3 bucket
    """
    #@batch(cpu=2, memory=3500,image=image)
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

        self.next(self.xgboost_regression)

    #@batch(cpu=2, memory=7500,image=image)
    @card
    @step
    def xgboost_regression(self):
        """Run XGBoost regression to predict score"""

        # Split training DataFrame into X_train and y_train
        X_train = self.training_df.drop(columns=["fid", "description", "score"])
        y_train = self.training_df["score"]
        # Fill None values in X_val with zeros
        X_train.fillna(0, inplace=True)
        # Fill None values in y_val with zeros
        y_train.fillna(0, inplace=True)

        # Convert y_val into a 1-dimensional array
        y_train = y_train.ravel()

        # Split validation DataFrame into X_val and y_val
        X_val = self.validation_df.drop(columns=["fid", "description", "score"])
        y_val = self.validation_df["score"]
        # Fill None values in X_val with zeros
        X_val.fillna(0, inplace=True)
        # Fill None values in y_val with zeros
        y_val.fillna(0, inplace=True)
        # Convert y_val into a 1-dimensional array
        y_val = y_val.ravel()
        # save artifacts
        self.X_val = X_val
        self.y_val = y_val

        # Define hyperparameters to try
        hyperparameters = {
            'n_estimators': [10, 20, 50, 100],
            'learning_rate': [0.1, 0.05, 0.01, 0.001],
            'max_depth': [3, 5, 7, 9],
            'subsample': [0.8, 0.9, 1.0],
            'colsample_bytree': [0.8, 0.9, 1.0],
        }

        # Initialize lists to store data for 3D plot
        learning_rates = []
        num_estimators = []
        r2_scores = []

        for n_estimators in hyperparameters['n_estimators']:
            for learning_rate in hyperparameters['learning_rate']:
                for max_depth in hyperparameters['max_depth']:
                    for subsample in hyperparameters['subsample']:
                        for colsample_bytree in hyperparameters['colsample_bytree']:
                            params = {
                                'n_estimators': n_estimators,
                                'learning_rate': learning_rate,
                                'max_depth': max_depth,
                                'subsample': subsample,
                                'colsample_bytree': colsample_bytree,
                                'objective': 'reg:squarederror',  # Change this to 'reg:squarederror' or 'reg:linear'
                                'random_state': 42
                            }

                            xgb_regressor = xgb.XGBRegressor(**params)
                            xgb_regressor.fit(X_train, y_train)
                            y_pred_val = xgb_regressor.predict(X_val)
                            val_score = r2_score(y_val, y_pred_val)

                            # Store data for 3D plot
                            learning_rates.append(learning_rate)
                            num_estimators.append(n_estimators)
                            r2_scores.append(val_score)

        best_r2_score = max(r2_scores)  # Find the highest R2 score
        best_index = r2_scores.index(best_r2_score)  # Find the index of the best R2 score

        # Find the corresponding best hyperparameters
        best_n_estimators = num_estimators[best_index]
        best_learning_rate = learning_rates[best_index]
        best_max_depth = max_depth
        best_subsample = subsample
        best_colsample_bytree = colsample_bytree

        # Create a DataFrame to display the results
        results = {
            'n_estimators': [best_n_estimators] * len(learning_rates),
            'learning_rate': learning_rates,
            'max_depth': [best_max_depth] * len(learning_rates),
            'subsample': [best_subsample] * len(learning_rates),
            'colsample_bytree': [best_colsample_bytree] * len(learning_rates),
            'R2 Score': r2_scores
        }
        self.results_df = pd.DataFrame(results).sort_values(by='R2 Score', ascending=False)

        # Save the best model and hyperparameters as an artifact
        best_xgb_regressor = xgb.XGBRegressor(
            n_estimators=best_n_estimators,
            learning_rate=best_learning_rate,
            max_depth=best_max_depth,
            subsample=best_subsample,
            colsample_bytree=best_colsample_bytree,
            objective='reg:squarederror',
            random_state=42
        )

        best_xgb_regressor.fit(X_train, y_train)
        self.xgboost_model = best_xgb_regressor
        
        # Create 3D plot
        fig = plt.figure(figsize=(10, 8))
        ax = fig.add_subplot(111, projection='3d')
        ax.scatter(learning_rates, num_estimators, r2_scores, c=r2_scores, cmap='viridis')
        ax.set_xlabel('Learning Rate')
        ax.set_ylabel('Number of Estimators')
        ax.set_zlabel('R-squared (R2) Score')
        ax.set_title('R2 Score vs. Learning Rate and Number of Estimators')
        current.card.append(Image.from_matplotlib(plt.gcf()))

        self.next(self.visualize)

    #@batch(cpu=2, memory=7500,image=image)
    @card
    @step
    def visualize(self):
        """Visualize XGBoost regression results and calculate MSE, RMSE, R2 score"""

        # Predict the 'score' for the validation data using the best model
        y_pred_val = self.xgboost_model.predict(self.X_val)

        # Convert y_val to a NumPy array
        y_val = self.y_val

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

        # Residuals vs. Predictors plots
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

    #@batch(cpu=2, memory=3500,image=image)
    @card
    @step
    def end(self):
        """Save the models to s3://{bucket_name}/{models}{model_name.joblib} files """
        # Save the model to a file
        self.xgboost_path = 'models/xgboost_model.joblib'

        # Create the directory if it doesn't exist
        os.makedirs('models', exist_ok=True)

        joblib.dump(self.xgboost_model, self.xgboost_path)

        # Upload the files to S3
        self.path_to_xgboost_model = upload_to_s3(self.xgboost_path, self.bucket_name)


if __name__ == '__main__':
    flow = SfEsXGBoostFlow()
    flow.run()
