"""Backward-compatible root entrypoint.

Railway production runs python -m app.mail. Running python main.py uses the
exact same application and Telegram allowlist. There is deliberately no
second monitor or independent notification sender in this file.
"""
import asyncio

from app.mail import main


if __name__ == "__main__":
    asyncio.run(main())
