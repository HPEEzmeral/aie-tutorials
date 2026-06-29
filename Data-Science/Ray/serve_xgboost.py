import os
import glob
import pickle
import xgboost
import pandas as pd
from ray import serve
from ray.train.xgboost import RayTrainReportCallback
from ray.train import Checkpoint
from starlette.requests import Request

storage_path = "/tmp/ray/storage/"
preprocessor_fname = "preprocessor.pkl"
preprocessor_path = os.path.join(storage_path, preprocessor_fname)
model_fname = "model.ubj"  # name used by XGBoost
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def get_checkpoint():
    # Find all ray_train_run directories, sorted by name (which includes timestamp)
    run_dirs = sorted(
        glob.glob(os.path.join(storage_path, "ray_train_run-*")), reverse=True
    )
    if not run_dirs:
        raise FileNotFoundError(f"No ray_train_run directories found in {storage_path}")

    for run_dir in run_dirs:
        checkpoint_dirs = sorted(
            [
                p
                for p in glob.glob(os.path.join(run_dir, "checkpoint_*"))
                if os.path.isdir(p)
            ],
            reverse=True,
        )
        if checkpoint_dirs:
            return checkpoint_dirs[0]  # Most recent checkpoint in this run

    raise FileNotFoundError(
        f"No checkpoints found in any ray_train_run directory under {storage_path}"
    )


def load_model_and_preprocessor():
    with open(preprocessor_path, "rb") as f:
        preprocessor = pickle.load(f)
    checkpoint_path = get_checkpoint()
    checkpoint = Checkpoint.from_directory(checkpoint_path)
    model = RayTrainReportCallback.get_model(checkpoint)
    return preprocessor, model


@serve.deployment(
    num_replicas=2, max_ongoing_requests=25, ray_actor_options={"num_cpus": 2}
)
class XGBoostModel:
    def __init__(self):
        self.preprocessor, self.model = load_model_and_preprocessor()

    @serve.batch(max_batch_size=16, batch_wait_timeout_s=0.1)
    async def predict_batch(self, input_data: list[dict]) -> list[float]:
        print(f"Batch size: {len(input_data)}")
        # Convert list of dictionaries to DataFrame.
        input_df = pd.DataFrame(input_data)
        # Preprocess the input.
        preprocessed_batch = self.preprocessor.transform_batch(input_df)
        # Create DMatrix for prediction.
        dmatrix = xgboost.DMatrix(preprocessed_batch)
        # Get predictions.
        predictions = self.model.predict(dmatrix)
        return predictions.tolist()

    async def __call__(self, request: Request):
        # Parse the request body as JSON.
        input_data = await request.json()
        return await self.predict_batch(input_data)


xgboost_model = XGBoostModel.bind()

print("Module binding complete: `xgboost_model` is available for `serve build`.")
