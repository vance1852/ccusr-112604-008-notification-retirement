"""Compatibility alias for legacy-line retirement callers."""

from retirement import *  # noqa: F401,F403
from retirement import RetirementEngine, RetirementService, analyze_timeline

__all__ = ["RetirementEngine", "RetirementService", "analyze_timeline"]
