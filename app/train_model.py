import pandas as pd
import joblib
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.metrics import confusion_matrix, classification_report, roc_auc_score, mean_squared_error, log_loss
from xgboost import XGBClassifier
from imblearn.over_sampling import SMOTE
import os

# Load dataset
csv_path = os.path.join(os.path.dirname(__file__), "D:/Projects/Datasets/creditcard.csv")
if not os.path.exists(csv_path):
    raise FileNotFoundError(f"CSV not found at {csv_path}")

credit_df = pd.read_csv(csv_path)
X = credit_df.drop(columns=["Class"])
y = credit_df["Class"]

# Apply SMOTE
smote = SMOTE(random_state=42)
X_resampled, y_resampled = smote.fit_resample(X, y)

# Split
X_train, X_test, y_train, y_test = train_test_split(X_resampled, y_resampled, test_size=0.2, random_state=42)


# Initialize an XGBClassifier with GPU support and no deprecated params
model = XGBClassifier(
    # Use GPU acceleration:
    tree_method='gpu_hist', 
    predictor='gpu_predictor',  # (Optional) explicitly use GPU for prediction
    # device='cuda',            # (Optional alternative in XGBoost >= 2.0)
    eval_metric='logloss',      # Set eval_metric to avoid warnings
    use_label_encoder=False,    # (If using XGBoost < 1.7, otherwise remove this param)
    random_state=42
)

# Hyperparameter tuning
param_grid = {
    'n_estimators': [100, 200],
    'max_depth': [4, 6, 8],
    'learning_rate': [0.1, 0.01]
}

# Set up GridSearchCV for hyperparameter tuning

model = XGBClassifier(use_label_encoder=False, eval_metric='logloss')
grid = GridSearchCV(model, param_grid, cv=5, scoring='roc_auc', n_jobs=-1,verbose=2)
grid.fit(X_train, y_train)
best_model = grid.best_estimator_

# Evaluate
y_pred = best_model.predict(X_test)
y_proba = best_model.predict_proba(X_test)[:, 1]
print("\n📊 Confusion Matrix:")
print(confusion_matrix(y_test, y_pred))
print("\n📈 Classification Report:")
print(classification_report(y_test, y_pred))
print(f"AUC: {roc_auc_score(y_test, y_proba):.4f}")
print(f"MSE: {mean_squared_error(y_test, y_proba):.4f}")
print(f"Log Loss: {log_loss(y_test, y_proba):.4f}")

# Save model + metadata
model_path = os.path.join(os.path.dirname(__file__), "model.pkl")
bundle = {
    "model": best_model,
    "features": list(X.columns)
}
joblib.dump(bundle, model_path)
print(f"✅ Optimized model saved to {model_path}")