"""Secret-fetch response DTO.

``GET /api/v3/worker/{worker_id}/secrets/{ref}`` resolves an opaque
``secret://...`` reference embedded in a route snapshot (per SEC-4 /
OB-COMP-1) into the raw secret material. The response is always
returned with ``Cache-Control: no-store, private`` so intermediates
cannot retain the body.
"""

from __future__ import annotations

from typing import Annotated

from pydantic import Field

from ai.backend.common.api_handlers import BaseResponseModel


class SecretFetchResponse(BaseResponseModel):
    """v3 secret-fetch response."""

    secret: Annotated[
        str,
        Field(
            description=(
                "Resolved raw secret material (typically a bearer token or signed URL). "
                "The caller MUST drop the value as soon as the upstream request is sent and "
                "MUST NOT persist it to disk."
            ),
        ),
    ]
