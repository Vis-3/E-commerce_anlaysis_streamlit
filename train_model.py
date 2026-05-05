"""
Churn Model Training Script
============================
Trains and compares Logistic Regression, Linear Discriminant Analysis, and
Gradient Boosting on RFM features. Saves the best model (by ROC-AUC) to disk.

Usage:
    python train_model.py [--output /tmp/churn_model.pkl]

Churn definition: no purchase in the last 90 days (recency_days > 90).
Features: recency_days, frequency, monetary, avg_order_value.
"""

import argparse
import logging
import os
import sys

import joblib
import numpy as np
import psycopg2
import psycopg2.extras
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, roc_auc_score
from sklearn.model_selection import StratifiedKFold, cross_val_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

CHURN_THRESHOLD_DAYS = 90
FEATURE_NAMES = ["recency_days", "frequency", "monetary", "avg_order_value"]


def get_connection():
    database_url = os.getenv("DATABASE_URL", "")
    if database_url:
        return psycopg2.connect(dsn=database_url)
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "ecommerce"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
    )


def fetch_training_data(conn) -> tuple[np.ndarray, np.ndarray]:
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT
            um.recency_days,
            um.frequency,
            um.monetary,
            um.avg_order_value,
            CASE WHEN um.recency_days > %s THEN 1 ELSE 0 END AS churned
        FROM user_metrics um
        WHERE um.frequency > 0
    """, (CHURN_THRESHOLD_DAYS,))
    rows = cur.fetchall()
    cur.close()

    X = np.array([[
        float(r["recency_days"]),
        float(r["frequency"]),
        float(r["monetary"]),
        float(r["avg_order_value"]),
    ] for r in rows])
    y = np.array([int(r["churned"]) for r in rows])
    return X, y


def build_candidates() -> dict:
    return {
        "LogisticRegression": Pipeline([
            ("scaler", StandardScaler()),
            ("clf", LogisticRegression(max_iter=1000, C=1.0, random_state=42)),
        ]),
        "LDA": Pipeline([
            ("scaler", StandardScaler()),
            ("clf", LinearDiscriminantAnalysis()),
        ]),
        "GradientBoosting": Pipeline([
            # GBM handles scale internally but we standardise for consistency
            ("scaler", StandardScaler()),
            ("clf", GradientBoostingClassifier(n_estimators=200, max_depth=4,
                                                learning_rate=0.05, random_state=42)),
        ]),
    }


def evaluate(name: str, pipeline, X: np.ndarray, y: np.ndarray) -> float:
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    aucs = cross_val_score(pipeline, X, y, cv=cv, scoring="roc_auc", n_jobs=-1)
    log.info("%-22s  AUC = %.4f ± %.4f", name, aucs.mean(), aucs.std())
    return aucs.mean()


def train_and_save(output_path: str) -> None:
    log.info("Connecting to database …")
    conn = get_connection()

    log.info("Fetching training data …")
    X, y = fetch_training_data(conn)
    conn.close()

    n_churned = y.sum()
    log.info("Dataset: %d users | %d churned (%.1f%%) | %d retained",
             len(y), n_churned, 100 * n_churned / len(y), len(y) - n_churned)

    if len(y) < 100:
        log.error("Not enough data to train a reliable model (need ≥ 100 users).")
        sys.exit(1)

    candidates = build_candidates()
    results = {}
    for name, pipeline in candidates.items():
        results[name] = evaluate(name, pipeline, X, y)

    best_name = max(results, key=results.get)
    best_auc  = results[best_name]
    log.info("Best model: %s (AUC %.4f)", best_name, best_auc)

    if best_auc < 0.55:
        log.warning("Best AUC %.4f is below threshold 0.55 — model may not be reliable.", best_auc)

    log.info("Fitting %s on full dataset …", best_name)
    best_pipeline = candidates[best_name]
    best_pipeline.fit(X, y)

    y_pred_prob = best_pipeline.predict_proba(X)[:, 1]
    y_pred      = (y_pred_prob >= 0.5).astype(int)
    log.info("Final train AUC: %.4f", roc_auc_score(y, y_pred_prob))
    log.info("\n%s", classification_report(y, y_pred, target_names=["Retained", "Churned"]))

    os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else ".", exist_ok=True)
    joblib.dump(best_pipeline, output_path)
    log.info("Model saved to %s", output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train churn prediction model")
    parser.add_argument("--output", default="/tmp/churn_model.pkl",
                        help="Path to save the trained model (default: /tmp/churn_model.pkl)")
    args = parser.parse_args()
    train_and_save(args.output)
