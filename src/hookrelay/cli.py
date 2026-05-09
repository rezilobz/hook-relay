"""Command-line entry point for HookRelay."""


def main() -> None:
    import uvicorn

    from hookrelay.config import settings

    uvicorn.run(
        "hookrelay.main:app",
        host=settings.host,
        port=settings.port,
        log_level=settings.log_level.lower(),
    )
