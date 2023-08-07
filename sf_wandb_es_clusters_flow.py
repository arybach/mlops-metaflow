import pandas as pd
import numpy as np
from elastic import get_elastic_client
from metaflow import FlowSpec, step, batch, environment, card, Parameter
from sklearn.cluster import AgglomerativeClustering
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score, calinski_harabasz_score
from sklearn.metrics import mean_squared_error, r2_score
from utils import upload_to_s3
from config import bucket_name, image
#import matplotlib.pyplot as plt
import plotly.graph_objects as go
import plotly.io as pio
import joblib
import io, os
import wandb
from wandb.integration.metaflow import wandb_log
from dotenv import load_dotenv

class SfWandbEsClustersFlow(FlowSpec):
    """
    A Metaflow flow for saving docs from ES nutrients index to s3 bucket
    """
    @environment(vars={"WANDB_API_KEY": os.getenv('WANDB_API_KEY')})
    @batch(cpu=2, memory=3500,image=image)
    @wandb_log(datasets=True, models=True, settings=wandb.Settings())
    @step
    def start(self):
        """fetch training docs from elastic search index"""

        # Force relogin to W&B
        self.bucket_name = bucket_name
    
        # Connect to Elasticsearch
        es = get_elastic_client("local")  # Update with your Elasticsearch configuration

        # Define the index name
        self.index_name = "labeled"
        scroll_size = 10000  # Adjust the scroll size based on your requirements

        # Define the query to retrieve all documents
        query = {
            "query": {
                "term": {
                    "label.keyword": "training"
                }
            },
            "size": scroll_size
        }
        # Initial request
        response = es.search(index=self.index_name, body=query, scroll="2m") # pylint: disable=E1123
        scroll_id = response["_scroll_id"]
        hits = response["hits"]["hits"]
        docs = hits

        # Subsequent requests
        while True:
            response = es.scroll(scroll_id=scroll_id, scroll="2m")
            scroll_id = response["_scroll_id"]
            hits = response["hits"]["hits"]
            if not hits:
                break
            docs.extend(hits)

        # Convert the documents to a pandas DataFrame
        self.training_df = pd.DataFrame([doc["_source"] for doc in docs])
        # wandb.log({"Training Dataframe": self.training_df})

        self.next(self.clusterize)

    @environment(vars={"WANDB_API_KEY": os.getenv('WANDB_API_KEY')})
    @wandb_log(datasets=True, models=True, settings=wandb.Settings())
    @batch(cpu=2, memory=7500,image=image)
    @step
    def clusterize(self):
        """ Clusterize training data with AgglomerativeClustering and CMeansClustering """
        
        train_df = self.training_df
        # Perform hierarchical clustering
        clustering_hierarchical = AgglomerativeClustering(n_clusters=10)
        cluster_labels_hierarchical = clustering_hierarchical.fit_predict(train_df[['score']])

        # Perform C-means clustering
        clustering_cmeans = KMeans(n_clusters=10)
        cluster_labels_cmeans = clustering_cmeans.fit_predict(train_df[['score']])

        # Add the cluster labels to the original DataFrame
        train_df['cluster_hierarchical'] = cluster_labels_hierarchical
        train_df['cluster_cmeans'] = cluster_labels_cmeans

        # View the resulting clusters
        self.clusters = train_df[['description', 'cluster_hierarchical', 'cluster_cmeans']]

        # Print cluster statistics
        self.counts = train_df['cluster_hierarchical'].value_counts()
        self.stats = train_df['cluster_hierarchical'].describe()

        # Log cluster labels for both hierarchical and c-means clustering
        wandb.log({"Cluster Labels (Hierarchical)": cluster_labels_hierarchical.tolist()})
        wandb.log({"Cluster Labels (CMeans)": cluster_labels_cmeans.tolist()})

        # Log the number of clusters used for both hierarchical and c-means clustering
        wandb.log({"Number of Clusters (Hierarchical)": 10})
        wandb.log({"Number of Clusters (CMeans)": 10})

        self.clustering_hierarchical = clustering_hierarchical
        self.clustering_cmeans = clustering_cmeans

        self.next(self.analyze)


    @environment(vars={"WANDB_API_KEY": os.getenv('WANDB_API_KEY')})
    @wandb_log(datasets=True, models=True, settings=wandb.Settings())
    @batch(cpu=2, memory=7500,image=image)
    @step
    def analyze(self):
        """ Analyze with WANDB Silhouette scores and Calinski-Harabasz scores vs the number of clusters to pick the right number of clusters """
    
        df = self.training_df
        num_clusters_range = list(range(2, 20))  # Convert range to a list
        silhouette_scores_kmeans = []
        silhouette_scores_agglo = []
        calinski_scores_kmeans = []
        calinski_scores_agglo = []

        for num_clusters in num_clusters_range:
            # KMeans
            clustering_kmeans = KMeans(n_clusters=num_clusters)
            kmeans_labels = clustering_kmeans.fit_predict(df[['score']])
            silhouette_scores_kmeans.append(silhouette_score(df[['score']], kmeans_labels))
            calinski_scores_kmeans.append(calinski_harabasz_score(df[['score']], kmeans_labels))

            # AgglomerativeClustering
            clustering_agglo = AgglomerativeClustering(n_clusters=num_clusters)
            agglo_labels = clustering_agglo.fit_predict(df[['score']])
            silhouette_scores_agglo.append(silhouette_score(df[['score']], agglo_labels))
            calinski_scores_agglo.append(calinski_harabasz_score(df[['score']], agglo_labels))

        wandb.log({"Silhouette Score (Hierarchical)": silhouette_scores_agglo,
                "Calinski-Harabasz Index (Hierarchical)": calinski_scores_agglo,
                "Silhouette Score (CMeans)": silhouette_scores_kmeans,
                "Calinski-Harabasz Index (CMeans)": calinski_scores_kmeans})

        # Convert lists to DataFrames
        silhouette_scores_df = pd.DataFrame({
            'Number of Clusters': num_clusters_range,
            'Silhouette Score (KMeans)': silhouette_scores_kmeans,
            'Silhouette Score (Agglomerative)': silhouette_scores_agglo
        })

        calinski_scores_df = pd.DataFrame({
            'Number of Clusters': num_clusters_range,
            'Calinski-Harabasz Index (KMeans)': calinski_scores_kmeans,
            'Calinski-Harabasz Index (Agglomerative)': calinski_scores_agglo
        })

        # Log DataFrames using WandB
        wandb.log({"Silhouette Scores vs Number of Clusters": silhouette_scores_df})
        wandb.log({"Calinski-Harabasz Index vs Number of Clusters": calinski_scores_df})

        self.next(self.fetch_validation_data)


    @environment(vars={"WANDB_API_KEY": os.getenv('WANDB_API_KEY')})
    @wandb_log(datasets=True, models=True, settings=wandb.Settings())
    @batch(cpu=2, memory=3500,image=image)
    @step
    def fetch_validation_data(self):
        """fetch validation docs from elastic search index"""
        
        es = get_elastic_client("local")  # Update with your Elasticsearch configuration
        scroll_size = 10000  # Adjust the scroll size based on your requirements

        # Define the query to retrieve all documents for validation
        validation_query = {
            "query": {
                "term": {
                    "label.keyword": "validation"
                }
            },
            "size": scroll_size
        }
        # Initial request
        response = es.search(index=self.index_name, body=validation_query, scroll="2m") # pylint: disable=E1123
        scroll_id = response["_scroll_id"]
        hits = response["hits"]["hits"]
        docs = hits

        # Subsequent requests
        while True:
            response = es.scroll(scroll_id=scroll_id, scroll="2m")
            scroll_id = response["_scroll_id"]
            hits = response["hits"]["hits"]
            if not hits:
                break
            docs.extend(hits)

        # Convert the documents to a pandas DataFrame
        self.validation_df = pd.DataFrame([doc["_source"] for doc in docs])
        self.next(self.validate)


    @environment(vars={"WANDB_API_KEY": os.getenv('WANDB_API_KEY')})
    @batch(cpu=2, memory=7500,image=image)
    @wandb_log(datasets=True, models=True, settings=wandb.Settings())
    @step
    def validate(self):
        """ Validate the clusters with the validation dataset """
        
        # Get validation DataFrame
        validation_df = self.validation_df

        # Predict clusters for the validation data using the trained models
        validation_df['cluster_hierarchical'] = self.clustering_hierarchical.fit_predict(validation_df[['score']])
        validation_df['cluster_cmeans'] = self.clustering_cmeans.fit_predict(validation_df[['score']])

        # View the resulting clusters
        self.validation_clusters = validation_df[['description', 'cluster_hierarchical', 'cluster_cmeans']]

        # Calculate residuals for the validation data
        y_pred_val = validation_df['score']
        y_val = validation_df['cluster_hierarchical']  # You can change this to 'cluster_cmeans' if needed
        residuals = y_val - y_pred_val

        # Plot residuals using Plotly
        fig_residuals = go.Figure()
        fig_residuals.add_trace(go.Histogram(x=residuals, nbinsx=30))
        fig_residuals.update_layout(
            xaxis_title='Residuals',
            yaxis_title='Frequency',
            title='Residual Distribution'
        )

        # Save the plot as a static image
        fig_residuals_path = "residual_distribution_plot.png"
        pio.write_image(fig_residuals, fig_residuals_path)

        # Log the residual plot as an image using wandb.Image
        wandb.log({"Residual Distribution": wandb.Image(fig_residuals_path)})

        # Calculate additional evaluation metrics
        wandb.log({"Validation MSE": mean_squared_error(y_val, y_pred_val),
                "Validation RMSE": np.sqrt(mean_squared_error(y_val, y_pred_val)),
                "Validation R2": r2_score(y_val, y_pred_val)})

        self.next(self.end)


    @environment(vars={"WANDB_API_KEY": os.getenv('WANDB_API_KEY')})
    @wandb_log(datasets=True, models=True, settings=wandb.Settings())
    @batch(cpu=2, memory=3500,image=image)
    @step
    def end(self):
        # Save the models to s3://{bucket_name}/{models}{model_name.joblib} files
    
        self.hierarchy_path = 'models/hierarchical_clustering_model_wandb.joblib'
        self.cmeans_path = 'models/cmeans_clustering_model_wandb.joblib'

        # Create the directory if it doesn't exist
        os.makedirs('models', exist_ok=True)

        joblib.dump(self.clustering_hierarchical, self.hierarchy_path)
        joblib.dump(self.clustering_cmeans, self.cmeans_path)

        # Upload the files to S3
        self.path_to_hierarchical_model = upload_to_s3(self.hierarchy_path, self.bucket_name)
        self.path_to_cmeans_model = upload_to_s3(self.cmeans_path, self.bucket_name)

if __name__ == '__main__':
    load_dotenv()
    # Initialize W&B
    wandb.init()
    flow = SfWandbEsClustersFlow()
    flow.run()
