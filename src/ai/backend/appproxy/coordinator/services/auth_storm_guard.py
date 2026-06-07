"""RC-6 mitigation: 401 storm guard.

When secrets.poll_token_secret rotates, every worker's first poll after
the rotation returns 401 (signature mismatch). Workers fall back to
bootstrap auth (api_secret) and re-register en masse. Without throttling,
this floods the registration endpoint and overwhelms Postgres.

This guard maintains a token bucket per (source_ip, worker_id) pair. When
the bucket is empty, the request is rejected with 429. Recovery is gentle
-- bucket refills at ``rate_per_sec`` so a single misbehaving worker can't
DoS its own registration but a healthy refresh storm spreads.

Used in two places:

1. ``worker_v3_auth_required``: when a token verification fails with
   ``AuthorizationFailed("Invalid poll token")``, the guard records the
   attempt. After N consecutive 401s within window W, subsequent calls
   from that worker return 429 until the bucket refills.
2. Bootstrap registration: same guard, separate bucket -- limits
   ``PUT /api/worker`` calls from a worker that just lost its poll_token.
"""

from __future__ import annotations

import logging
from typing import Any

from ai.backend.logging import BraceStyleAdapter

log = BraceStyleAdapter(logging.getLogger(__spec__.name))


_DEFAULT_MAX_BURST = 5
_DEFAULT_WINDOW_SECONDS = 60


class AuthStormGuard:
    """Token-bucket style guard backed by Valkey ``INCR`` + ``EXPIRE``.

    The implementation uses a fixed-window counter rather than a true
    leaky-bucket: the counter is created on the first 401 inside the
    window with a TTL of ``window_seconds`` and is incremented on every
    subsequent 401. Once the counter exceeds ``max_burst`` within the
    window, :meth:`record_and_check` returns ``False`` so the caller can
    respond with HTTP 429.

    A fixed-window counter is sufficient here because the goal is to
    flatten a synchronised storm (every worker hitting the coordinator at
    the same instant after a secret rotation) rather than to enforce a
    precise long-term rate. Once the window expires, the counter is
    dropped and the worker may try again -- aligning with the worker-side
    :class:`BootstrapBackoff` which adds randomised jitter.
    """

    def __init__(
        self,
        valkey_client: Any,
        max_burst: int = _DEFAULT_MAX_BURST,
        window_seconds: int = _DEFAULT_WINDOW_SECONDS,
    ) -> None:
        if max_burst <= 0:
            raise ValueError("max_burst must be positive")
        if window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        self._valkey = valkey_client
        self._max_burst = max_burst
        self._window_seconds = window_seconds

    @property
    def max_burst(self) -> int:
        return self._max_burst

    @property
    def window_seconds(self) -> int:
        return self._window_seconds

    async def record_and_check(self, scope_key: str) -> bool:
        """Record a 401 for ``scope_key`` and check the bucket.

        :param scope_key: A namespaced key identifying the bucket. The
            recommended convention is ``f"401:{source_ip}:{worker_id}"``
            for poll-token failures and ``f"bootstrap:{source_ip}:{worker_id}"``
            for bootstrap re-registration attempts. The caller is
            responsible for sanitising the components.
        :returns: ``True`` if the request may proceed (bucket has
            capacity remaining), ``False`` if the caller must respond
            with HTTP 429. On any Valkey error the guard fails *open*
            and returns ``True`` -- the storm guard is a best-effort
            mitigation and must not itself become a single point of
            failure for the auth path.
        """
        key = self._namespaced(scope_key)
        try:
            current = await self._valkey.incr(key)
            if current == 1:
                # First hit inside this window -- arm the TTL so the
                # bucket eventually drains.
                await self._valkey.expire(key, self._window_seconds)
        except Exception as e:
            log.warning(
                "AuthStormGuard: Valkey error on key={}: {!r}; failing open",
                key,
                e,
            )
            return True

        if current > self._max_burst:
            log.warning(
                "AuthStormGuard: bucket exhausted for {} (count={}, max={})",
                scope_key,
                current,
                self._max_burst,
            )
            return False
        return True

    @staticmethod
    def _namespaced(scope_key: str) -> str:
        return f"proxy-coord.auth-storm:{scope_key}"
