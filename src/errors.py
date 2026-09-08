"""Failure taxonomy for the consumer.

The whole retry/DLQ decision hangs on one question: *can this message ever
succeed if we try again?*

  TransientError -> yes, maybe. Retry with exponential backoff, DLQ only when
                    the retry budget is exhausted.
  PermanentError -> no. Retrying a malformed or invalid record just burns the
                    budget and blocks the partition, so it goes to the DLQ on
                    the first attempt.
"""

from __future__ import annotations


class ProcessingError(Exception):
    """Base class for anything that stops an order from being processed."""


class TransientError(ProcessingError):
    """A retryable failure (downstream timeout, 503, connection reset...)."""


class PermanentError(ProcessingError):
    """A non-retryable failure (schema/validation/business-rule violation)."""
