"""Heartbeat request / response DTOs.

The heartbeat request body is :class:`WorkerSelfReport` (re-exported via
``ai.backend.appproxy.common.dto.worker_contract``). Both the v2 legacy
endpoint ``PATCH /api/worker/{id}`` and the v3 endpoint
``PATCH /api/v3/worker/{id}/heartbeat`` accept the same body shape; the
legacy endpoint additionally accepts an EMPTY body (``Content-Length: 0``)
as the OB-OPS-3 backward-compatible path, which the coordinator
normalises to a default-zero :class:`WorkerSelfReport`.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated

from pydantic import Field

from ai.backend.common.api_handlers import BaseResponseModel


class WorkerHeartbeatResponse(BaseResponseModel):
    """v3 heartbeat response (``PATCH /api/v3/worker/{id}/heartbeat``).

    The coordinator acknowledges the heartbeat and may revise the
    worker's poll cadence on the fly via ``poll_interval_hint_ms`` —
    workers SHOULD honour the hint when next computing their poll
    timer (jitter is layered on top of the hint, not the worker's
    own default).
    """

    ack_at: Annotated[
        datetime, Field(description="Coordinator clock when the heartbeat was acknowledged.")
    ]
    poll_interval_hint_ms: Annotated[
        int,
        Field(
            ge=0,
            description=(
                "Advisory next-poll interval in milliseconds. Workers SHOULD use this as the base "
                "before applying client-side jitter so the coordinator can dampen poll storms by "
                "lowering the hint cluster-wide."
            ),
        ),
    ]
