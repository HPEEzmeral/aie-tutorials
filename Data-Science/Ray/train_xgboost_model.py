import os
import sys
import pandas as pd
import pickle
import time
import xgboost
import ray
from ray.data import Dataset
from ray.data.preprocessors import StandardScaler
from ray.train.xgboost import RayTrainReportCallback, XGBoostTrainer
from ray.train import CheckpointConfig, Result, RunConfig, ScalingConfig

sys.path.append(os.path.abspath(".."))

# Make Ray data less verbose.
ray.data.DataContext.get_current().enable_progress_bars = True
ray.data.DataContext.get_current().print_on_execution_start = True


def prepare_data() -> tuple[Dataset, Dataset, Dataset]:
    """Load and split the dataset into train, validation, and test sets."""
    dataset = ray.data.read_csv("breast_cancer.csv")
    seed = 42

    # Split 70% for training.
    train_dataset, rest = dataset.train_test_split(
        test_size=0.3, shuffle=True, seed=seed
    )
    # Split the remaining 30% into 15% validation and 15% testing.
    valid_dataset, test_dataset = rest.train_test_split(
        test_size=0.5, shuffle=True, seed=seed
    )
    return train_dataset, valid_dataset, test_dataset


start = time.perf_counter()
# Load and split the dataset.
train_dataset, valid_dataset, _test_dataset = prepare_data()
train_dataset.take(1)
end = time.perf_counter()
print(f"Elapsed time: {end - start:.6f} seconds")

if os.path.exists("/tmp/ray/storage/"):
    storage_path = "/tmp/ray/storage/"
    print(f"Using tmp ray storage path: {storage_path}")
else:
    storage_path = "/tmp/ray/storage"
    print(f"/tmp/ray/storage/ not available, creating dir: {storage_path}")
    os.makedirs(storage_path, exist_ok=True)

preprocessor_fname = "preprocessor.pkl"
preprocessor_path = os.path.join(storage_path, preprocessor_fname)
model_fname = "model.ubj"  # name used by XGBoost
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def train_preprocessor(train_dataset: ray.data.Dataset) -> StandardScaler:
    # Pick some dataset columns to scale.
    columns_to_scale = [c for c in train_dataset.columns() if c != "target"]

    # Initialize the preprocessor.
    preprocessor = StandardScaler(columns=columns_to_scale)
    # Train the preprocessor on the training set.
    preprocessor.fit(train_dataset)

    return preprocessor


preprocessor = train_preprocessor(train_dataset)

with open(preprocessor_path, "wb") as f:
    pickle.dump(preprocessor, f)

train_dataset = preprocessor.transform(train_dataset)
valid_dataset = preprocessor.transform(valid_dataset)
train_dataset.take(1)

# Configure checkpointing to save progress during training.
run_config = RunConfig(
    checkpoint_config=CheckpointConfig(
        # Checkpoint every 10 iterations.
        # Only keep the latest checkpoint.
        num_to_keep=1,
    ),
    ## For multi-node clusters, configure storage that's accessible
    ## across all worker nodes with `storage_path="s3://..."`.
    storage_path=storage_path,
)

NUM_WORKERS = 1
USE_GPU = True


def train_fn_per_worker(config: dict):
    """Training function that runs on each worker.

    This function:
    1. Gets the dataset shard for this worker
    2. Converts to pandas for XGBoost
    3. Separates features and labels
    4. Creates DMatrix objects
    5. Trains the model using distributed communication
    """
    # Get this worker's dataset shard.
    train_ds, val_ds = (
        ray.train.get_dataset_shard("train"),
        ray.train.get_dataset_shard("validation"),
    )

    # Materialize the data and convert to pandas.
    train_ds = train_ds.materialize().to_pandas()
    val_ds = val_ds.materialize().to_pandas()

    # Separate the labels from the features.
    train_X, train_y = train_ds.drop("target", axis=1), train_ds["target"]
    eval_X, eval_y = val_ds.drop("target", axis=1), val_ds["target"]

    # Convert the data into DMatrix format for XGBoost.
    dtrain = xgboost.DMatrix(train_X, label=train_y)
    deval = xgboost.DMatrix(eval_X, label=eval_y)

    # Do distributed data-parallel training.
    # Ray Train sets up the necessary coordinator processes and
    # environment variables for workers to communicate with each other.
    _booster = xgboost.train(
        config["xgboost_params"],
        dtrain=dtrain,
        evals=[(dtrain, "train"), (deval, "validation")],
        num_boost_round=10,
        # Handles metric logging and checkpointing.
        callbacks=[RayTrainReportCallback()],
    )


# Parameters for the XGBoost model.
model_config = {
    "xgboost_params": {
        "tree_method": "auto",
        "objective": "binary:logistic",
        "eval_metric": ["logloss", "error"],
    }
}

trainer = XGBoostTrainer(
    train_fn_per_worker,
    train_loop_config=model_config,
    # Register the data subsets.
    datasets={"train": train_dataset, "validation": valid_dataset},
    # See "Scaling strategies" for more details.
    scaling_config=ScalingConfig(
        # Number of workers for data parallelism.
        num_workers=NUM_WORKERS,
        # Set to True to use GPU acceleration.
        use_gpu=USE_GPU,
    ),
    run_config=run_config,
)

print("Starting training...")
result: Result = trainer.fit()
print("Training complete.\nResult:", result)

metrics = result.metrics
print("Metrics:", metrics)
