# Databricks notebook source
# MAGIC %md
# MAGIC ### Airline Delay Prediction Model

# COMMAND ----------

# MAGIC %md
# MAGIC #### Setup

# COMMAND ----------

# MAGIC %run ./utils

# COMMAND ----------

import pandas as pd
import seaborn as sb
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import confusion_matrix
from sklearn.metrics import roc_auc_score
from sklearn.svm import SVC
from sklearn import linear_model

# COMMAND ----------

# MAGIC %md
# MAGIC # Databricks ML Platform
# MAGIC ![mlflow-overview](https://databricks.com/wp-content/uploads/2021/05/lakehouse-architecture-1-1.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # MLFlow Process Flow overview
# MAGIC ![mlflow-overview](https://databricks.com/wp-content/uploads/2020/04/databricks-adds-access-control-to-mlflow-model-registry_01.jpg)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Feature Engineering
# MAGIC 
# MAGIC ##### Databricks Feature Store can be used to populate feature catalogs + feature tables

# COMMAND ----------

flights = pd.read_csv('/dbfs/mnt/deepaksekaradls/Airlines_Data/flights.csv')
flights

# COMMAND ----------

flights_sample_data = flights.sample(frac=0.05)

# COMMAND ----------

sb.jointplot(data=flights_sample_data, x="SCHEDULED_ARRIVAL", y="ARRIVAL_TIME")

# COMMAND ----------

plt.figure(figsize=(12, 12))
display(sb.heatmap(flights_sample_data.corr(), cbar=True, square=True, fmt='.1f', annot=True, annot_kws={'size': 5}, cmap='Greens'))

# COMMAND ----------

flights_sample_data.corr()

# COMMAND ----------

# filtering out unnecessary columns
flights_sample_data=flights_sample_data.drop(['YEAR','FLIGHT_NUMBER','AIRLINE','DISTANCE','TAIL_NUMBER','TAXI_OUT','SCHEDULED_TIME','DEPARTURE_TIME','WHEELS_OFF','ELAPSED_TIME','AIR_TIME','WHEELS_ON','DAY_OF_WEEK','TAXI_IN','CANCELLATION_REASON'],axis=1)

# COMMAND ----------

flights_sample_data=flights_sample_data.fillna(flights_sample_data.mean())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Label the dataset

# COMMAND ----------

result=[]

for row in flights_sample_data['ARRIVAL_DELAY']:
  if row > 0 and row < 10:
    result.append(1)
  elif row >= 10 and row < 20:
    result.append(2)
  elif row >= 20 and row < 30:
    result.append(3) 
  else:
    result.append(4) 
    
flights_sample_data['result'] = result

# COMMAND ----------

flights_sample_data

# COMMAND ----------

flights_sample_data.value_counts('result')

# COMMAND ----------

# removing some more columns
flights_sample_data=flights_sample_data.drop(['ORIGIN_AIRPORT', 'DESTINATION_AIRPORT','ARRIVAL_TIME', 'ARRIVAL_DELAY']
                                             ,axis=1)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Store the labeled dataset as a Delta Table

# COMMAND ----------

# Enable Arrow-based columnar data transfers
spark.conf.set("spark.sql.execution.arrow.enabled", "true")

# Create a Spark DataFrame from a pandas DataFrame using Arrow
spark_df = spark.createDataFrame(flights_sample_data)

spark_df.write.format("delta").mode("overwrite").saveAsTable("deepak_va.flights_labelled_data")

# COMMAND ----------

# MAGIC %md
# MAGIC # AutoML
# MAGIC ![mlflow-overview](https://media-exp3.licdn.com/dms/image/C5612AQE80XhiruH6FA/article-inline_image-shrink_1000_1488/0/1558412666884?e=1630540800&v=beta&t=UPSZOEAJ9rz2Dt9slNQvOVCZBZGIFIW_xDGEWVV8JrI)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Use AutoML UI or Code

# COMMAND ----------

data = flights_sample_data.values
X, y = data[:,:-1], data[:,-1]
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.30, random_state=42)  # splitting in the ratio 70:30

scaled_features = StandardScaler().fit_transform(X_train, X_test)

# COMMAND ----------

#summary = databricks.automl.classify(X_train,target_col='result', timeout_minutes = 5, primary_metric = 'accuracy')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Or get your hands dirty

# COMMAND ----------

experiment_config = get_or_create_experiment(PROJECT_PATH, 'Flight Delay Prediction - Manual')
experiment_path = experiment_config['experiment_path']
experiment_id = experiment_config['experiment_id']

mlflow.set_experiment(experiment_path)
displayHTML(f"<h2>Make sure you can see your experiment on <a href='#mlflow/experiments/{experiment_id}'>#mlflow/experiments/{experiment_id}</a></h2>")

# COMMAND ----------

dbutils.widgets.text("Run Name", "First Run")
dbutils.widgets.text("Exp Id","")

run_name = dbutils.widgets.get("Run Name")
experiment_id = dbutils.widgets.get("Exp Id")

run_name, experiment_id

# COMMAND ----------

with mlflow.start_run(run_name=run_name, experiment_id=experiment_id):
  mlflow.sklearn.autolog()
  mlflow.spark.autolog()
 
  # training a DecisionTreeClassifier
  dtree_model = DecisionTreeClassifier(max_depth = 2).fit(X_train, y_train)
  dtree_predictions = dtree_model.predict(X_test)
  
  # creating a confusion matrix
  cm = confusion_matrix(y_test, dtree_predictions)
  cm

# COMMAND ----------

with mlflow.start_run(run_name=run_name, experiment_id=experiment_id):
  mlflow.sklearn.autolog()
  mlflow.spark.autolog()
  # training a KNN classifier
  from sklearn.neighbors import KNeighborsClassifier
  knn = KNeighborsClassifier(n_neighbors = 7).fit(X_train, y_train)

# COMMAND ----------

# accuracy on X_test
accuracy = knn.score(X_test, y_test)
print(accuracy)                                      

# creating a confusion matrix
knn_predictions = knn.predict(X_test) 
cm = confusion_matrix(y_test, knn_predictions)
