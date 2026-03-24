"""Database schema migrations registry."""

from .v1_initial import apply_v1_initial_schema
from .v2_rebound_365d import apply_v2_rebound_365d_schema

MIGRATIONS = (
    (1, apply_v1_initial_schema),
    (2, apply_v2_rebound_365d_schema),
)

LATEST_SCHEMA_VERSION = MIGRATIONS[-1][0] if MIGRATIONS else 0
