# Bike Sharing (MLFlow - KServe)

![bike-sharing](images/bike-sharing.jpg)

This tutorial provides a detailed walkthrough of a comprehensive data science
workflow, encompassing data preprocessing, model training and evaluation,
hyperparameter tuning, experiment tracking via MLFlow, and model deployment
using KServe. The use case under consideration is the well-known
bike sharing dataset, sourced from the UCI Machine Learning Repository.


The dataset records the hourly and daily count of rental bikes between 2011 and
2012 in the Capital Bikeshare system, supplemented with corresponding weather
and seasonal data. The primary objective of this dataset is to foster research
into bike sharing systems, which are gaining significant attention due to their
implications on traffic management, environmental sustainability, and public
health.

The task associated with this dataset is regression, with 17,389 instances. The
overarching goal is to construct a predictive model capable of forecasting bike
rental demand.

## What You'll Need

To complete the tutorial follow the steps below:

* Login to your AIE cluster.
* Connect to notebook server instance. For this example select default-notebook.
* Clone the repository locally.
* Launch the `bike-sharing-mlflow.ipynb` notebook file and follow the
  instructions.
* After the successful completion of the previous step, launch the
  `bike-sharing-prediction.ipynb` file to invoke the model you deployed.

> It is recommended to create a Notebook server with at least 3CPUs and 4Gi of memory
