"""Run deterministic tests and a historical dry-run before deployment."""
import asyncio
import logging
import sys
import unittest

from app import backtest


def main():
    logging.basicConfig(
        level="INFO",
        format="%(asctime)s %(levelname)s %(message)s",
    )
    suite = unittest.defaultTestLoader.discover("tests")
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    if not result.wasSuccessful():
        sys.exit(1)

    logging.info("UNIT TESTS PASSED. Starting historical Futures backtest.")
    asyncio.run(backtest.main())


if __name__ == "__main__":
    main()
