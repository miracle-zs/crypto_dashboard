"""Database schema migrations registry."""

from .v1_initial import apply_v1_initial_schema
from .v2_rebound_365d import apply_v2_rebound_365d_schema
from .v3_daily_klines_and_cursors import apply_v3_daily_klines_and_cursors
from .v4_first_principles_facts_and_snapshots import apply_v4_first_principles_facts_and_snapshots
from .v5_open_lots_persistence import apply_v5_open_lots_persistence

MIGRATIONS = (
    (1, apply_v1_initial_schema),
    (2, apply_v2_rebound_365d_schema),
    (3, apply_v3_daily_klines_and_cursors),
    (4, apply_v4_first_principles_facts_and_snapshots),
    (5, apply_v5_open_lots_persistence),
)

LATEST_SCHEMA_VERSION = MIGRATIONS[-1][0] if MIGRATIONS else 0

