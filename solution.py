"""Convenience import alias for environments expecting ``solution``."""

from retirement import *  # noqa: F401,F403
from retirement import RetirementEngine, RetirementService, analyze_timeline

RetirementCoordinator = RetirementEngine

__all__ = ["RetirementEngine", "RetirementService", "RetirementCoordinator", "analyze_timeline"]
