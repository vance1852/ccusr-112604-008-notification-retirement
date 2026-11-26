"""Compatibility alias for the core coordinator."""

from .engine import RetirementEngine as RetirementCoordinator
from .engine import RetirementEngine

Coordinator = RetirementEngine
__all__ = ["RetirementCoordinator", "RetirementEngine", "Coordinator"]
