def get_secrets_client():
    from .apiclient import SecretsApiClient

    return SecretsApiClient()


def get(name: str) -> str:
    """Get project secret."""
    return get_secrets_client().get(name)
