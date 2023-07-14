import pandas as pd
from elastic import get_elastic_client
from metaflow import FlowSpec, step, card, current, Parameter
from metaflow.cards import Image
from sklearn.cluster import AgglomerativeClustering
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score, calinski_harabasz_score
from utils import upload_to_s3
import matplotlib.pyplot as plt
import joblib
import io, os

class SfEsClustersFlow(FlowSpec):
    """
    A Metaflow flow for saving docs from ES nutrients index to s3 bucket
    """
    @step
    def start(self):
        """fetch training docs from elastic search index"""

        self.bucket_name = "mlops-nutrients"
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
 
        self.next(self.clusterize)

    @card
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
        self.clustering_hierarchical = clustering_hierarchical
        self.clustering_cmeans = clustering_cmeans

        self.next(self.visualize)

    # python jupyter/sf_es_clusters_flow.py card view visualize
    # on the left side of the VS code explorer under metaflow_card_cache right click on the card and open in the browser
    @card
    @step
    def visualize(self):
        """ Visualize Silhouette scores and Calinski-Harabasz scores vs the number of clusters to pick the right number of clusters """
        df = self.training_df
        num_clusters_range = range(2, 20)
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

        # Create a new figure for Silhouette Score vs Number of Clusters
        plt.figure()
        # Plot Silhouette Score vs Number of Clusters for KMeans
        plt.plot(num_clusters_range, silhouette_scores_kmeans, label='KMeans')
        # Plot Silhouette Score vs Number of Clusters for AgglomerativeClustering
        plt.plot(num_clusters_range, silhouette_scores_agglo, label='AgglomerativeClustering')
        plt.xlabel('Number of Clusters')
        plt.ylabel('Silhouette Score')
        plt.title('Silhouette Score vs Number of Clusters')
        plt.legend()

        # Save the Silhouette Score plot as an image and append it to the card
        fig_silhouette = plt.gcf()
        current.card.append(Image.from_matplotlib(fig_silhouette))

        # Create a new figure for Calinski-Harabasz Index vs Number of Clusters
        plt.figure()
        # Plot Calinski-Harabasz Index vs Number of Clusters for KMeans
        plt.plot(num_clusters_range, calinski_scores_kmeans, label='KMeans')
        # Plot Calinski-Harabasz Index vs Number of Clusters for AgglomerativeClustering
        plt.plot(num_clusters_range, calinski_scores_agglo, label='AgglomerativeClustering')
        plt.xlabel('Number of Clusters')
        plt.ylabel('Calinski-Harabasz Index')
        plt.title('Calinski-Harabasz Index vs Number of Clusters')
        plt.legend()

        # Save the Calinski-Harabasz Index plot as an image and append it to the card
        fig_calinski = plt.gcf()
        current.card.append(Image.from_matplotlib(fig_calinski))

        self.next(self.end)

    @step
    def end(self):
        # Save the models to s3://{bucket_name}/{models}{model_name.pkl} files
        self.hierarchy_path = 'models/hierarchical_clustering_model.pkl'
        self.cmeans_path = 'models/cmeans_clustering_model.pkl'

        # Create the directory if it doesn't exist
        os.makedirs('models', exist_ok=True)

        joblib.dump(self.clustering_hierarchical, self.hierarchy_path)
        joblib.dump(self.clustering_cmeans, self.cmeans_path)

        # Upload the files to S3
        self.path_to_hierarchical_model = upload_to_s3(self.hierarchy_path, self.bucket_name)
        self.path_to_cmeans_model = upload_to_s3(self.cmeans_path, self.bucket_name)


if __name__ == '__main__':
    flow = SfEsClustersFlow()
    flow.run()
