"""Errors raised by the legacy-line retirement coordinator."""

from __future__ import annotations


class RetirementError(Exception):
    """Base class for all domain errors."""


class ValidationError(RetirementError, ValueError):
    """A command or source record does not contain the required facts."""


class NotFoundError(RetirementError, KeyError):
    """A referenced consumer, ticket, operation, or resource is unknown."""


class RevisionConflict(RetirementError):
    """An optimistic concurrency check rejected a stale expected revision."""

    def __init__(self, expected: int, actual: int):
        self.expected = expected
        self.actual = actual
        super().__init__(f"state revision {expected} is stale; current revision is {actual}")


class RollbackBlocked(RetirementError):
    """A rollback cannot be performed as a plain restore."""
