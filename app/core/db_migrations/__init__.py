"""Database schema migrations registry."""

from .v1_initial import apply_v1_initial_schema

MIGRATIONS = (
    (1, apply_v1_initial_schema),
)

LATEST_SCHEMA_VERSION = MIGRATIONS[-1][0] if MIGRATIONS else 0
