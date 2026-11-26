"""Local retirement coordination for a legacy notification line."""

from .engine import RetirementEngine, fresh_state
from .errors import NotFoundError, RetirementError, RevisionConflict, ValidationError
from .service import RetirementService, analyze_timeline, load_inventory, load_jsonl
from .synthetic import LegacyLine, Line, NotificationReceiver, Receiver, SyntheticLine, SyntheticReceiver

__all__ = [
    "RetirementEngine",
    "RetirementService",
    "SyntheticLine",
    "SyntheticReceiver",
    "Line",
    "Receiver",
    "LegacyLine",
    "NotificationReceiver",
    "analyze_timeline",
    "fresh_state",
    "load_inventory",
    "load_jsonl",
    "RetirementError",
    "ValidationError",
    "NotFoundError",
    "RevisionConflict",
]

__version__ = "1.0.0"

# Descriptive aliases for callers/tests that name the use case differently.
RetirementCoordinator = RetirementEngine
Coordinator = RetirementEngine
MigrationCoordinator = RetirementEngine
RetirementPlanner = RetirementEngine
RetirementSystem = RetirementEngine
NotificationRetirement = RetirementEngine
RetirementStore = RetirementService
LegacyLineRetirement = RetirementService
