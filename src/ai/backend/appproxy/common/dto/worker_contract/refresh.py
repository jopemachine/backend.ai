"""Refresh-token response DTO.

``POST /api/v3/worker/{id}/refresh-token`` bumps ``poll_token_revision``
(invalidating every outstanding token for the worker) and returns a
freshly-issued bearer bound to the new revision. Per RC-4, a Valkey
single-flight lock serialises concurrent refresh requests so that
racing callers see the SAME revision instead of the row being bumped
twice.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated

from pydantic import Field

from ai.backend.common.api_handlers import BaseResponseModel


class RefreshTokenResponse(BaseResponseModel):
    """v3 refresh-token response."""

    token: Annotated[
        str,
        Field(description="Freshly-issued poll_token JWT, replaces the caller's previous Bearer."),
    ]
    expires_at: Annotated[
        datetime,
        Field(
            description="Absolute expiry of the new ``token``. Caller schedules its next refresh before this."
        ),
    ]
