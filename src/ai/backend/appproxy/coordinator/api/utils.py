import functools
from collections.abc import Callable
from typing import Any, Literal
from uuid import UUID

import jwt
from aiohttp import web
from aiohttp.typedefs import Handler

from ai.backend.appproxy.common.errors import AuthorizationFailed
from ai.backend.appproxy.common.utils import set_handler_attr
from ai.backend.appproxy.coordinator.models.worker import Worker
from ai.backend.appproxy.coordinator.services.poll_token import PollTokenService
from ai.backend.appproxy.coordinator.types import RootContext


def auth_required(
    scope: Literal["manager"] | Literal["worker"] | Literal["worker_v3"],
) -> Callable[[Handler], Handler]:
    def wrap(handler: Handler) -> Handler:
        @functools.wraps(handler)
        async def wrapped(request: web.Request, *args: Any, **kwargs: Any) -> web.StreamResponse:
            root_ctx: RootContext = request.app["_root.context"]
            permitted_token = root_ctx.local_config.secrets.api_secret
            permitted_header_values = (
                permitted_token,
                f"Bearer {permitted_token}",
                f"BackendAI {permitted_token}",
            )
            if _token := request.headers.get("X-BackendAI-Token"):
                if _token not in permitted_header_values:
                    raise AuthorizationFailed("Unauthorized access")
            elif _auth_header := request.headers.get("Authorization"):
                method, _, token = _auth_header.partition(" ")
                match method.lower():
                    case "bearer":
                        try:
                            payload = jwt.decode(
                                token,
                                root_ctx.local_config.secrets.api_secret,
                                algorithms=["HS256"],
                                options={"require": ["exp", "iat", "scope"]},
                            )
                        except jwt.PyJWTError as e:
                            raise AuthorizationFailed("Unauthorized access") from e
                        # Scope is enforced — a token issued for "worker"
                        # scope cannot be replayed against "manager"
                        # endpoints (and vice versa). Without this the
                        # signature alone is sufficient to access any path,
                        # which is what the original R4 finding flagged.
                        token_scope = payload.get("scope")
                        if token_scope != scope:
                            raise AuthorizationFailed(
                                f"Token scope mismatch: required={scope}, got={token_scope}"
                            )
                    case _:
                        raise AuthorizationFailed(f"Unsupported authorization method {method}")
            else:
                raise AuthorizationFailed("Unauthorized access")
            return await handler(request, *args, **kwargs)

        original_attrs = getattr(handler, "_backend_attrs", {})
        for k, v in original_attrs.items():
            set_handler_attr(wrapped, k, v)

        set_handler_attr(wrapped, "auth_scope", scope)
        return wrapped

    return wrap


def worker_v3_auth_required(handler: Handler) -> Handler:
    """Stricter auth decorator for v3 polling endpoints.

    Differences from :func:`auth_required` (which guards v1/v2 endpoints):

    * Decodes the ``Bearer`` token using
      ``root_ctx.local_config.secrets.poll_token_secret`` instead of the shared
      ``api_secret``. This isolates the v3 poll-token blast radius from the
      generic API token (mitigates SEC-1 — separate signing key).
    * Enforces ``aud == "appproxy-coordinator-v3"``, ``algorithms=["HS256"]``
      and a 60-second clock-skew leeway on ``exp``/``iat``/``nbf``.
    * Cross-checks the token's ``sub`` (subject) against the path's
      ``worker_id`` so a token issued for worker A cannot be replayed against
      worker B's polling endpoint (mitigates SEC-2 — cross-worker replay).
    * Loads ``workers.poll_token_revision`` from the database and rejects the
      request when ``token.token_revision`` is out of sync. Rotating the
      column bumps the revision so previously-issued tokens are revoked
      instantly without waiting for ``exp``.
    * Performs a ``jti`` replay check against Valkey via
      :meth:`PollTokenService.check_jti_replay` (mitigates SEC-replay —
      one-shot token reuse).

    The validated payload is stashed on ``request["poll_token_payload"]`` so
    downstream handlers can read worker scope/capabilities without re-decoding
    the token.
    """

    @functools.wraps(handler)
    async def wrapped(request: web.Request, *args: Any, **kwargs: Any) -> web.StreamResponse:
        root_ctx: RootContext = request.app["_root.context"]

        auth_header = request.headers.get("Authorization")
        if not auth_header:
            raise AuthorizationFailed("Missing Authorization header")

        method, _, token = auth_header.partition(" ")
        if method.lower() != "bearer" or not token:
            raise AuthorizationFailed("Bearer token required for v3 polling endpoints")

        # SEC-2: Bind the token to the worker identified by the URL path
        # BEFORE we trust the token's claims. ``sub`` will be re-checked
        # inside PollTokenService.verify against expected_worker_id below.
        path_worker_id_raw = request.match_info.get("worker_id")
        if not path_worker_id_raw:
            raise AuthorizationFailed("worker_id missing from request path")
        try:
            path_worker_id = UUID(path_worker_id_raw)
        except ValueError as e:
            raise AuthorizationFailed("Invalid worker_id in path") from e

        # Revocation gate: load the workers row to read the currently-valid
        # poll_token_revision. PollTokenService.verify rejects tokens whose
        # revision is stale, instantly revoking older tokens on operator bump.
        async with root_ctx.db.begin_readonly_session() as sess:
            worker = await Worker.get(sess, path_worker_id)
            db_revision = worker.poll_token_revision

        # SEC-1: Decode against the dedicated poll_token_secret rather than
        # the broader api_secret. PollTokenService is the single source of
        # truth for the v3 audience / algorithm / leeway / sub / revision /
        # kid validation policy.
        svc = PollTokenService(
            jwt_secret=root_ctx.local_config.secrets.poll_token_secret,
        )
        try:
            payload = svc.verify(token, path_worker_id, int(db_revision))
        except AuthorizationFailed:
            raise
        except jwt.PyJWTError as e:
            raise AuthorizationFailed("Invalid poll token") from e

        # The polling Bearer is multi-use by design (heartbeat/get_routes are
        # called repeatedly during the token's lifetime), so jti replay is
        # enforced only on refresh-token issuance, not on every poll.
        request["poll_token_payload"] = payload
        return await handler(request, *args, **kwargs)

    original_attrs = getattr(handler, "_backend_attrs", {})
    for k, v in original_attrs.items():
        set_handler_attr(wrapped, k, v)

    set_handler_attr(wrapped, "auth_scope", "worker_v3")
    return wrapped
