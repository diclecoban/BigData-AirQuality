"""Feature engineering skeleton for AQI forecasting."""


def add_lag_features(df):
    """TODO: Add lag-based predictors such as t-1, t-3, and t-24."""
    raise NotImplementedError


def add_rolling_statistics(df):
    """TODO: Add rolling mean and rolling std features."""
    raise NotImplementedError


def add_time_features(df):
    """TODO: Encode hour, month, weekend, and cyclical time signals."""
    raise NotImplementedError


def add_spatial_features(df):
    """TODO: Add district and location-derived features."""
    raise NotImplementedError


def build_feature_dataset(df):
    """TODO: Compose the final model-ready feature table."""
    raise NotImplementedError
