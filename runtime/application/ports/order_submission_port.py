"""
Deprecated alias — use :mod:`risk_order_intent_submission_port` instead.
"""

from runtime.application.ports.risk_order_intent_submission_port import (
    RiskOrderIntentSubmissionPort,
)

OrderSubmissionPort = RiskOrderIntentSubmissionPort

__all__ = ["OrderSubmissionPort", "RiskOrderIntentSubmissionPort"]
