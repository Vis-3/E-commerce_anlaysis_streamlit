import logging
import os
from typing import Optional

log = logging.getLogger(__name__)

_churn_model = None
_model_loaded = False

FEATURE_NAMES = ["recency_days", "frequency", "monetary", "avg_order_value"]

FEATURE_LABELS = {
    "recency_days":     "days since last purchase",
    "frequency":        "total orders",
    "monetary":         "total spend",
    "avg_order_value":  "average order value",
}


def load_churn_model(model_path: str = "/tmp"):
    """Load churn_model.pkl from disk. Returns None if not found."""
    global _churn_model, _model_loaded

    if _model_loaded:
        return _churn_model

    try:
        import joblib
        path = os.path.join(model_path, "churn_model.pkl")
        if not os.path.exists(path):
            log.warning("Churn model not found at %s — predictions will be unavailable", path)
            _model_loaded = True
            return None

        _churn_model = joblib.load(path)
        _model_loaded = True
        log.info("Churn model loaded from %s", path)
        return _churn_model

    except Exception as exc:
        log.warning("Failed to load churn model: %s", exc)
        _model_loaded = True
        return None


def predict_churn(model, features: dict) -> tuple[float, str]:
    """
    Returns (probability, label) where label is Low / Medium / High.
    Falls back to a heuristic if model is None.
    """
    if model is not None:
        try:
            import numpy as np
            X = [[features.get(f, 0) for f in FEATURE_NAMES]]
            prob = float(model.predict_proba(X)[0][1])
        except Exception as exc:
            log.warning("Model inference failed: %s — using heuristic", exc)
            prob = _heuristic_churn(features)
    else:
        prob = _heuristic_churn(features)

    if prob >= 0.7:
        label = "High"
    elif prob >= 0.4:
        label = "Medium"
    else:
        label = "Low"

    return round(prob, 4), label


def get_risk_factors(model, features: dict) -> list[str]:
    """
    Return top-2 human-readable risk factors using model coefficients.
    Falls back to rule-based factors when model is unavailable.
    """
    if model is not None:
        try:
            import numpy as np
            coefs = model.coef_[0]
            feature_values = [features.get(f, 0) for f in FEATURE_NAMES]

            # Contribution = coefficient × normalised feature value
            contributions = []
            for i, (fname, coef, val) in enumerate(zip(FEATURE_NAMES, coefs, feature_values)):
                contributions.append((abs(coef * val), coef, fname, val))

            contributions.sort(reverse=True)
            return [_format_factor(fname, val, coef > 0)
                    for _, coef, fname, val in contributions[:2]]
        except Exception:
            pass

    return _rule_based_factors(features)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _heuristic_churn(features: dict) -> float:
    recency = features.get("recency_days", 365)
    frequency = features.get("frequency", 1)
    recency_score = min(1.0, recency / 365)
    freq_score = 1.0 - min(1.0, frequency / 20)
    return round((recency_score * 0.6 + freq_score * 0.4), 4)


def _format_factor(feature: str, value: float, increases_risk: bool) -> str:
    direction = "High" if increases_risk else "Low"
    label = FEATURE_LABELS.get(feature, feature)
    value_str = f"{int(value)}" if feature in ("recency_days", "frequency") else f"${value:.0f}"
    return f"{direction} {label} ({value_str})"


def _rule_based_factors(features: dict) -> list[str]:
    factors = []
    recency = features.get("recency_days", 0)
    frequency = features.get("frequency", 0)
    monetary = features.get("monetary", 0)

    if recency > 90:
        factors.append(f"High recency ({recency} days since last purchase)")
    if frequency < 3:
        factors.append(f"Low purchase frequency ({frequency} orders)")
    if monetary < 100:
        factors.append(f"Low total spend (${monetary:.0f})")

    return factors[:2] if factors else ["Insufficient purchase history"]
