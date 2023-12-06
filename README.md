# Optimizing-MLOps-for-Predictive-Models

<b>Abstract:</b>

In this project we develop a real-time prediction system and an architecture to monitor the model and detect drift in the data. We utilize the openly available US accidents kaggle dataset for our project. The problem we solve in this dataset is to predict the severity of an accident given various environmental features like temperature, humidity, location details etc. We build a streaming ETL pipeline using Spark that not only ingests data from Apache-kafka into the databricks cluster but also does the real time prediction. Then we aim to maintain this model by constantly monitoring it using MLFlow, that keeps track of machine learning experiments, and data drift detection to keep a check on the quality of data at regular intervals. By building such a robust system that adds a lot of business values like, we can reduce the cost of retraining an ML model by taking an informed decision, the model predictions can help enhance emergency response efficiency, informs traffic management, aids in risk assessment for insurance processes, and supports public safety initiatives.

<b> Architecture: </b>
https://github.com/avsk80/Optimizing-MLOps-for-Predictive-Models/blob/main/msba-project-arch.jpg

This project repository is created in partial fulfillment of the requirements for the Big Data Analytics course offered by the Master of Science in Business Analytics program at the Carlson School of Management, University of Minnesota
