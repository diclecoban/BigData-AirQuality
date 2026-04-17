"""Primary GBT model training skeleton."""


def load_training_dataset():
    """TODO: Load the model-ready dataset for GBT training."""
    raise NotImplementedError


def train_gbt_regressor(train_df):
    """TODO: Train the main Gradient Boosted Trees model."""
    raise NotImplementedError


def tune_gbt_hyperparameters(train_df):
    """TODO: Add grid or manual hyperparameter search."""
    raise NotImplementedError


def register_best_model(model):
    """TODO: Register the best GBT model in MLflow or storage."""
    raise NotImplementedError
