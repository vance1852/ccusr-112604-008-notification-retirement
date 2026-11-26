"""Top-level compatibility module: ``from retirement_coordinator import ...``."""

from retirement import *  # noqa: F401,F403
from retirement import (  # noqa: F401
    RetirementEngine,
    RetirementService,
    SyntheticLine,
    SyntheticReceiver,
    analyze_timeline,
)

RetirementCoordinator = RetirementEngine
Coordinator = RetirementEngine

__all__ = ["RetirementEngine", "RetirementCoordinator", "Coordinator", "RetirementService",
           "SyntheticLine", "SyntheticReceiver", "analyze_timeline"]
