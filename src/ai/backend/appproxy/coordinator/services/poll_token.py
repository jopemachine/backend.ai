"""Polling-token issuance and verification for worker mode.

This service issues short-lived HS256 JWTs that workers use to authenticate
their long-polling requests against the coordinator. The verifier enforces
audience binding, subject pinning (SEC-2: the token's ``sub`` must match the
worker_id taken from the request path), and a per-worker monotonic token
revision so that an operator-initiated revoke (bumping the revision) takes
effect on the very next call. A Valkey-backed ``jti`` replay guard
(``RC-replay-guard``) prevents the same token from being accepted twice
inside a small race window.

The class is intentionally framework-agnostic: it accepts a Valkey client
that exposes ``set(key, value, nx=True, ex=...)`` and returns a truthy value
on success / ``None`` on a key collision (the contract honoured by every
Valkey wrapper in the codebase).
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from secrets import token_hex
from typing import Any
from uuid import UUID, uuid4

import jwt

from ai.backend.appproxy.common.errors import AuthorizationFailed
from ai.backend.logging import BraceStyleAdapter

log = BraceStyleAdapter(logging.getLogger(__spec__.name))


_DEFAULT_TTL = timedelta(hours=24)
_DEFAULT_LEEWAY = 60
_ALGORITHM = "HS256"
_ISSUER = "appproxy-coordinator"
_AUDIENCE = "appproxy-coordinator-v3"
_CURRENT_KID = "v1"
_JTI_KEY_PREFIX = "proxy-worker.jti:"
_REFRESH_LOCK_KEY_PREFIX = "proxy-worker.refresh-lock:"
_REQUIRED_CLAIMS = [
    "exp",
    "iat",
    "nbf",
    "sub",
    "aud",
    "scope",
    "token_revision",
    "jti",
    "kid",
]


class PollTokenService:
    """Issue + verify polling JWTs handed out to appproxy workers."""

    _jwt_secret: str
    _ttl: timedelta
    _leeway: int

    def __init__(
        self,
        jwt_secret: str,
        ttl: timedelta = _DEFAULT_TTL,
        leeway: int = _DEFAULT_LEEWAY,
    ) -> None:
        if not jwt_secret:
            # A token signed with an empty secret would be trivially forgeable;
            # refuse to start rather than silently degrade security.
            raise ValueError("PollTokenService requires a non-empty jwt_secret")
        # `token_hex` is imported per spec; we tie it into the audit log so a
        # fresh service instance is distinguishable in concurrent test runs.
        self._jwt_secret = jwt_secret
        self._ttl = ttl
        self._leeway = leeway
        log.debug(
            "PollTokenService initialised (ttl={}s, leeway={}s, instance={})",
            int(ttl.total_seconds()),
            leeway,
            token_hex(4),
        )

    def issue(
        self,
        worker_id: UUID,
        scope: dict[str, Any],
        token_revision: int,
    ) -> tuple[str, datetime]:
        """Mint a polling token for ``worker_id``.

        Returns the encoded JWT and the absolute ``exp`` instant so callers
        can persist it (e.g. into the workers table) without re-decoding.
        """
        now = datetime.now(UTC)
        exp = now + self._ttl
        payload: dict[str, Any] = {
            "iss": _ISSUER,
            "aud": _AUDIENCE,
            "sub": str(worker_id),
            "scope": scope,
            "token_revision": token_revision,
            "jti": uuid4().hex,
            "iat": now,
            "nbf": now,
            "exp": exp,
            "kid": _CURRENT_KID,
        }
        token = jwt.encode(payload, self._jwt_secret, algorithm=_ALGORITHM)
        log.debug(
            "issued poll token for worker {} (revision={}, exp={})",
            worker_id,
            token_revision,
            exp.isoformat(),
        )
        return token, exp

    def verify(
        self,
        token: str,
        expected_worker_id: UUID,
        expected_revision: int,
    ) -> dict[str, Any]:
        """Decode + validate a polling token.

        Beyond what PyJWT's signature/exp checks cover, we enforce:

        * ``sub`` matches the worker_id taken from the URL path (SEC-2). A
          valid-but-foreign token must never authenticate calls against
          another worker's resource.
        * ``token_revision`` matches the worker's current revision; bumping
          the column in the DB instantly invalidates every outstanding
          token without waiting for ``exp``.
        * ``kid`` is the one we currently mint with. This gives us a rolling
          upgrade path: a future ``v2`` signer can be deployed in parallel
          while still rejecting unknown/forged kids.
        """
        try:
            payload: dict[str, Any] = jwt.decode(
                token,
                self._jwt_secret,
                algorithms=[_ALGORITHM],
                audience=_AUDIENCE,
                leeway=self._leeway,
                options={"require": _REQUIRED_CLAIMS},
            )
        except jwt.PyJWTError as e:
            # Don't leak which check failed (sig vs exp vs aud) to the caller;
            # the appproxy log still has the detail for ops.
            log.debug("poll token rejected by PyJWT: {!r}", e)
            raise AuthorizationFailed("Invalid polling token") from e

        sub = payload.get("sub")
        if sub != str(expected_worker_id):
            # SEC-2: path-level worker_id wins. Refuse cross-worker replay.
            log.warning(
                "poll token sub mismatch: token_sub={}, path_worker_id={}",
                sub,
                expected_worker_id,
            )
            raise AuthorizationFailed("Token sub != path worker_id")

        revision = payload.get("token_revision")
        if revision != expected_revision:
            log.info(
                "poll token revision stale for worker {}: token_rev={}, current_rev={}",
                expected_worker_id,
                revision,
                expected_revision,
            )
            raise AuthorizationFailed("Token revision stale (was revoked)")

        kid = payload.get("kid")
        if kid != _CURRENT_KID:
            log.warning("poll token kid rejected: got={}, expected={}", kid, _CURRENT_KID)
            raise AuthorizationFailed("Unknown kid")

        return payload

    async def check_jti_replay(
        self,
        jti: str,
        exp: datetime,
        valkey_client: Any,
    ) -> bool:
        """Single-use guard for a token's ``jti``.

        Returns ``True`` if this is the first time we see ``jti`` (and the
        key was claimed), ``False`` if it has already been observed inside
        the token's remaining lifetime (treated as replay by the caller).

        We use ``SET key value NX EX <ttl>`` so the guard auto-expires when
        the token itself does — no separate sweeper required.
        """
        now = datetime.now(UTC)
        # ``exp`` may arrive as naive UTC from PyJWT depending on version;
        # normalise so the subtraction below is always tz-aware.
        if exp.tzinfo is None:
            exp = exp.replace(tzinfo=UTC)
        ttl_seconds = int((exp - now).total_seconds())
        if ttl_seconds <= 0:
            # Token is already past its expiry — let the caller treat this
            # as a replay rather than poisoning Valkey with a zero-TTL key.
            log.debug("jti {} has non-positive ttl ({}s); treating as replay", jti, ttl_seconds)
            return False

        key = f"{_JTI_KEY_PREFIX}{jti}"
        # `nx=True` means the call only succeeds the *first* time the jti is
        # presented; every subsequent attempt within `ttl_seconds` returns
        # a falsy value and is flagged as a replay.
        result = await valkey_client.set_if_not_exists(key, "1", ex=ttl_seconds)
        if not result:
            log.warning("jti replay detected: {}", jti)
            return False
        return True

    async def acquire_refresh_lock(
        self,
        worker_id: UUID,
        valkey_client: Any,
        *,
        lock_ttl_ms: int = 5000,
    ) -> bool:
        """Single-flight guard for poll_token refresh (RC-4 mitigation).

        Acquires a short-lived Valkey lock so only ONE refresh-token request per
        worker_id makes it through at a time. If two refresh calls land
        simultaneously (e.g. PollTokenRefresher + Continuum's AsyncAuthStrategy
        both noticed expiry), the loser sees False and must re-read the freshly
        bumped revision from the DB rather than bumping again.

        Returns True if the caller owns the lock (proceed with bump+issue);
        False if another caller already owns it (caller should re-fetch the
        current token via DB).

        Lock auto-expires after lock_ttl_ms so a crashed holder never blocks
        forever. The TTL is intentionally short — bump+issue is a single DB
        round trip.
        """
        key = f"{_REFRESH_LOCK_KEY_PREFIX}{worker_id}"
        # The lock value carries a per-attempt nonce so debug logs can
        # disambiguate concurrent attempts; only the *winner* writes it. We do
        # not implement explicit release — relying on the short TTL keeps the
        # protocol robust against crashes/timeouts of the lock holder.
        nonce = token_hex(8)
        # Prefer millisecond precision via ``px`` when the client supports it;
        # fall back to ``ex`` (seconds) for clients that only expose the
        # coarser knob. Every Valkey wrapper in the codebase accepts ``ex``;
        # ``px`` is the optional refinement.
        try:
            result = await valkey_client.set(key, nonce, nx=True, px=lock_ttl_ms)
        except TypeError:
            # Older client signature without ``px`` — round up to whole seconds
            # so we never under-shoot the requested TTL.
            ex_seconds = max(1, (lock_ttl_ms + 999) // 1000)
            result = await valkey_client.set(key, nonce, nx=True, ex=ex_seconds)

        if not result:
            # Collisions are expected when PollTokenRefresher and an opportunistic
            # caller race; surface at debug to help diagnose stampedes without
            # spamming production logs.
            log.debug(
                "poll-token refresh lock collision for worker {} (nonce={})",
                worker_id,
                nonce,
            )
            return False
        log.debug(
            "poll-token refresh lock acquired for worker {} (nonce={}, ttl_ms={})",
            worker_id,
            nonce,
            lock_ttl_ms,
        )
        return True
