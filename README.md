# Team 1: Optimizing-MLOps-for-Predictive-Models

<b>Abstract:</b>

In this project we develop a real-time prediction system and an architecture to monitor the model and detect drift in the data. We utilize the openly available US accidents kaggle dataset for our project. The problem we solve in this dataset is to predict the severity of an accident given various environmental features like temperature, humidity, location details etc. We build a streaming ETL pipeline using Spark that not only ingests data from Apache-kafka into the databricks cluster but also does the real time prediction. Then we aim to maintain this model by constantly monitoring it using MLFlow, that keeps track of machine learning experiments, and data drift detection to keep a check on the quality of data at regular intervals. By building such a robust system that adds a lot of business values like, we can reduce the cost of retraining an ML model by taking an informed decision, the model predictions can help enhance emergency response efficiency, informs traffic management, aids in risk assessment for insurance processes, and supports public safety initiatives.

<b> Architecture: </b>

![alt text](https://github.com/avsk80/Optimizing-MLOps-for-Predictive-Models/blob/main/msba-project-arch.jpg)

<b> Technical flow of Project: </b>

1) Our raw dataset of 500k in JSON format is stored in S3 manually.
2) A pyspark producer running in Databricks publishes data from S3 to Kafka cluster in GCP.
3) A pyspark consumer running in Databrciks reads data and does real-time prediction for each micro-batch before writing to a delta-table.
4) Insert data to a final hive table for further analytics usecases like model building and data drift detection.
5) Our real-time system is assisted by 2 batch processes. They are data drift process to monitor incoming data distributions, and the model building process is tracked by MLFlow.

<b> Highlights of our project: </b>

1) Robust Data Engineering pipeline - Our data pipeline is scalable, reliable, replayable (implemented upserts to handle duplicates in real-time).
2) Alerting mechanism - Our drift detection process is capable of notify the users of the detected data drift.
3) Model Monitoring - Keep track of model experiments using MLFlow.
4) Scalable ML - Developed and Deployed Logistic Regression and Random Forest Models in pyspark that were trained in a distributed setup.

<b> Business Impact: </b>

1) The drift detection process helps up make an informed decision on when to retrain a ML Model rather than retrain a model in a brute-force approach. This saves valuable compute resources and enables efficient cluster maintanace.
2) MFlow's model tracking, versioning can alleviate the pain involved in maintaining multiple versions of different models. Thus saves a lot of time by providing automatic logging system.

<b> Project Setup requirements: </b>
We recommend to create a databricks account (preferrably enterprise edition to leverage MLFlow's full services).

Create a confluent Kafka account: https://shorturl.at/erA03


Python packages used in our project: pyspark, evidently, smtplib, pandas, numpy.

command to install: pip install <package_name>.

Maven installations needed: org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0

<b> Flyer: </b>
https://github.com/avsk80/Optimizing-MLOps-for-Predictive-Models/blob/main/project-flyer.pdf

<b> Sample images of Project Work: </b>

![alt text](https://github.com/avsk80/Optimizing-MLOps-for-Predictive-Models/blob/main/kafka-sample.png)
View of messages stored in a Kafka Topic.


![alt text](https://github.com/avsk80/Optimizing-MLOps-for-Predictive-Models/blob/main/mlflow-exp-sample.png)
MLFlow experiments tab.


![alt text](https://github.com/avsk80/Optimizing-MLOps-for-Predictive-Models/blob/main/mlflow-model-params-chart.png)
MLFlow model parameters comparision chart.


![alt text](https://github.com/avsk80/Optimizing-MLOps-for-Predictive-Models/blob/main/mlflow-model-deployment.png)
MLFlow model deplyoment in Production.


![alt text](https://github.com/avsk80/Optimizing-MLOps-for-Predictive-Models/blob/main/drift-detection-sample.png)
Drift Detection sample report.


Project Video:


Citations:
1) https://docs.evidentlyai.com/presets/data-drift
2) https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
3) https://kafka.apache.org/documentation/
4) https://api-docs.databricks.com/python/pyspark/latest/pyspark.ml.html
5) https://medium.com/geekculture/integrate-kafka-with-pyspark-f77a49491087

<i> This project repository is created in partial fulfillment of the requirements for the Big Data Analytics course offered by the Master of Science in Business Analytics program at the Carlson School of Management, University of Minnesota </i>
