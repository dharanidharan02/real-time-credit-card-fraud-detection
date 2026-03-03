import pandas as pd
import joblib
from sklearn.datasets import make_classification
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
import os

# Generate dummy binary classification dataset
X, y = make_classification(n_samples=500, n_features=31, random_state=42)

# Train/test split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

# Train a basic model
model = LogisticRegression()
model.fit(X_train, y_train)

# Save the model
model_path = os.path.join(os.path.dirname(__file__), "model.pkl")
joblib.dump(model, model_path)

print(f"✅ Model saved at {model_path}")
