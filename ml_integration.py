"""
Minimal ML integration stubs to satisfy imports and provide extension points
for signal enhancement, exit override optimization, and ML-based position sizing.

This module intentionally provides lightweight async functions and placeholder
objects so the application can boot and later be extended with real models
without changing call sites.
"""

from typing import Any, Dict


class _StubModelManager:
    def __init__(self):
        self.models = {}


class _Noop:
    async def __call__(self, *args, **kwargs):
        return {"status": "noop"}


# Public objects expected by main.py
ml_model_manager = _StubModelManager()
ml_signal_enhancer = _Noop()
ml_exit_override_optimizer = _Noop()
ml_position_sizer = _Noop()


async def enhance_tradingview_signal(signal: Dict[str, Any]) -> Dict[str, Any]:
    """Return the signal with a default confidence and no changes (stub)."""
    return {
        "enhanced": True,
        "base_signal": signal,
        "confidence": 0.5,
        "notes": "ML stub - no enhancement applied"
    }


async def should_override_exit(position: Dict[str, Any], exit_signal: Dict[str, Any]) -> Dict[str, Any]:
    """Suggest not to override by default (stub)."""
    return {
        "override": False,
        "confidence": 0.5,
        "reason": "ML stub - no override logic configured"
    }


async def calculate_ml_position_size(signal: Dict[str, Any], base_size: float) -> Dict[str, Any]:
    """Return the base size unchanged (stub)."""
    return {
        "recommended_size": base_size,
        "multiplier": 1.0,
        "confidence": 0.5,
        "notes": "ML stub - size unchanged"
    }


