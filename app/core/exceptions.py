"""Shared exception definitions used across the Collinear backend."""

class SupabaseClientError(RuntimeError):
    """Raised when the application cannot create or communicate with Supabase."""


class RateLimitExceededError(RuntimeError):
    """Raised by rate-limiting middleware when a client exceeds its quota.""" 