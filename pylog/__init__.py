"""pylog — a Kafka-style durable, partitioned, replicated commit log with Raft consensus."""

from .log import CommitLog, CorruptRecord, Record, Segment

__all__ = ["CommitLog", "CorruptRecord", "Record", "Segment"]
__version__ = "0.1.0"
