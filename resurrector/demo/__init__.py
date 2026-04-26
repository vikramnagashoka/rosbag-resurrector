"""Synthetic demo / sample data generators that ship with the package.

Originally lived under ``tests/fixtures/`` for use by the test suite,
but `resurrector demo`, the dashboard's "Generate demo bag" button,
and the exploration scripts all need it at runtime — so it had to
move into the installed package surface.

The test suite still uses these via ``from resurrector.demo.sample_bag
import ...``; a back-compat shim under ``tests/fixtures/`` re-exports
them so historical imports keep working.
"""

from resurrector.demo.sample_bag import BagConfig, generate_bag

__all__ = ["BagConfig", "generate_bag"]
