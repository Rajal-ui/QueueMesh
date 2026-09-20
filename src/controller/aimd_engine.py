"""
QueueMesh - AIMD Engine
Pure Python mathematical implementation of Additive Increase / Multiplicative Decrease
rate-limiting control loop adapted for AWS Lambda Event Source Mapping concurrency.
"""

import math
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class AIMDResult:
    """Represents the output evaluation of an AIMD cycle."""
    target_concurrency: int
    previous_concurrency: int
    action: str  # "MULTIPLICATIVE_DECREASE" | "ADDITIVE_INCREASE" | "NO_OP"
    next_state: str  # "HEALTHY" | "THROTTLED" | "CRITICAL_BACKOFF" | "RECOVERING"
    consecutive_breaches: int
    reason: str


class AIMDEngine:
    """
    Additive Increase / Multiplicative Decrease (AIMD) Controller.
    
    Closed-loop feedback control algorithm adapted from TCP congestion control:
      - Multiplicative Decrease: C_next = max(C_min, floor(C_current * Beta))
      - Additive Increase:       C_next = min(C_max, C_current + Alpha)
    """

    def __init__(
        self,
        t1_threshold: float = 0.05,
        t2_threshold: float = 0.98,
        alpha: int = 5,
        beta: float = 0.50,
        c_min: int = 2,
        c_max: int = 100,
    ) -> None:
        """
        Initializes the AIMD engine with threshold and tuning parameters.

        :param t1_threshold: Error rate threshold (429 rate) triggering backoff (default 0.05 / 5%)
        :param t2_threshold: Success rate threshold (200 rate) triggering recovery (default 0.98 / 98%)
        :param alpha: Additive increase step size in worker count (default 5)
        :param beta: Multiplicative decrease backoff factor (default 0.50)
        :param c_min: Minimum concurrency floor allowed by AWS ESM (default 2)
        :param c_max: Maximum baseline concurrency ceiling (default 100)
        """
        if not (0.0 <= t1_threshold <= 1.0):
            raise ValueError(f"T1 threshold must be between 0.0 and 1.0, got {t1_threshold}")
        if not (0.0 <= t2_threshold <= 1.0):
            raise ValueError(f"T2 threshold must be between 0.0 and 1.0, got {t2_threshold}")
        if not (0.0 < beta < 1.0):
            raise ValueError(f"Beta must be between 0.0 and 1.0, got {beta}")
        if alpha < 1:
            raise ValueError(f"Alpha must be >= 1, got {alpha}")
        if c_min < 2:
            raise ValueError(f"C_min must be >= 2 per AWS SQS ESM requirements, got {c_min}")
        if c_max < c_min:
            raise ValueError(f"C_max ({c_max}) cannot be less than C_min ({c_min})")

        self.t1_threshold = t1_threshold
        self.t2_threshold = t2_threshold
        self.alpha = alpha
        self.beta = beta
        self.c_min = c_min
        self.c_max = c_max

    def calculate(
        self,
        current_concurrency: int,
        error_rate: float,
        success_rate: float,
        consecutive_breaches: int = 0,
    ) -> AIMDResult:
        """
        Calculates the target concurrency and next state based on error and success rates.

        :param current_concurrency: Current SQS ESM MaximumConcurrency setting
        :param error_rate: Current downstream HTTP 429/error rate (0.0 - 1.0)
        :param success_rate: Current downstream HTTP 200 rate (0.0 - 1.0)
        :param consecutive_breaches: Counter of consecutive cycles breaching T1
        :return: AIMDResult containing next target concurrency, state, action, and reasoning
        """
        # Clamp current concurrency within valid bounds before calculation
        current_c = max(self.c_min, min(self.c_max, int(current_concurrency)))

        # Rule 1: HTTP 429 Rate exceeds or equals T1 threshold -> Multiplicative Decrease
        if error_rate >= self.t1_threshold:
            new_breaches = consecutive_breaches + 1
            calculated_concurrency = math.floor(current_c * self.beta)
            new_concurrency = max(self.c_min, calculated_concurrency)

            # State transition logic per PRD Section 9
            if new_breaches >= 2 or new_concurrency <= self.c_min:
                next_state = "CRITICAL_BACKOFF"
            else:
                next_state = "THROTTLED"

            action = "MULTIPLICATIVE_DECREASE"
            reason = (
                f"HTTP 429 breach: Error rate {error_rate:.1%} >= T1 ({self.t1_threshold:.1%}). "
                f"Applied factor Beta={self.beta}. Scaled from {current_c} -> {new_concurrency}."
            )
            return AIMDResult(
                target_concurrency=new_concurrency,
                previous_concurrency=current_c,
                action=action,
                next_state=next_state,
                consecutive_breaches=new_breaches,
                reason=reason,
            )

        # Rule 2: Low error rate and sustained high success rate -> Additive Increase
        if error_rate < self.t1_threshold and success_rate >= self.t2_threshold:
            new_concurrency = min(self.c_max, current_c + self.alpha)
            new_breaches = 0

            if new_concurrency >= self.c_max:
                next_state = "HEALTHY"
            else:
                next_state = "RECOVERING"

            action = "ADDITIVE_INCREASE"
            reason = (
                f"Downstream healthy: Success rate {success_rate:.1%} >= T2 ({self.t2_threshold:.1%}) "
                f"and Error rate {error_rate:.1%} < T1 ({self.t1_threshold:.1%}). "
                f"Added Alpha=+{self.alpha}. Scaled from {current_c} -> {new_concurrency}."
            )
            return AIMDResult(
                target_concurrency=new_concurrency,
                previous_concurrency=current_c,
                action=action,
                next_state=next_state,
                consecutive_breaches=new_breaches,
                reason=reason,
            )

        # Rule 3: Plateau / Neutral zone (error < T1, but success < T2) -> No-Op
        new_concurrency = current_c
        new_breaches = consecutive_breaches
        next_state = "RECOVERING" if current_c < self.c_max else "HEALTHY"
        action = "NO_OP"
        reason = (
            f"Within operational tolerance: Error rate {error_rate:.1%}, Success rate {success_rate:.1%}. "
            f"Concurrency maintained at {current_c}."
        )

        return AIMDResult(
            target_concurrency=new_concurrency,
            previous_concurrency=current_c,
            action=action,
            next_state=next_state,
            consecutive_breaches=new_breaches,
            reason=reason,
        )
