"""Back-compat shim — the real generator now lives in
``resurrector.demo.sample_bag`` so it ships with the installed
package (needed by `resurrector demo`, the dashboard's
"Generate demo bag" button, and the exploration scripts).

Tests historically did ``from tests.fixtures.generate_test_bags
import generate_bag, BagConfig``; this module preserves that import
path while delegating to the package module.

New code should import directly:

    from resurrector.demo.sample_bag import generate_bag, BagConfig
"""

from resurrector.demo.sample_bag import BagConfig, generate_bag

__all__ = ["BagConfig", "generate_bag"]
