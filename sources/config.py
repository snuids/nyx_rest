from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict

_ENV_FILE = Path(__file__).parent / ".env"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(_ENV_FILE),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Application
    WELCOMEMESSAGE: str = ""
    ICON: str = ""

    # Legal details of the data protection notice. Left empty, they display
    # "à compléter"; filled in, they replace the text in all three languages without
    # rebuilding the image — restarting the API is enough.
    PRIVACY_CONTROLLER: str = ""
    PRIVACY_ADDRESS: str = ""
    PRIVACY_CONTACT: str = ""

    # Feature flags
    USE_LOGSTASH: bool = False
    COOKIESECURE: bool = True

    # Client extensions to load at startup, comma separated module names of `sources/extensions/`
    # (see extensions/README.md). Empty means none: `lib/` used to be scanned for any file
    # containing "ext" and its `config()` called, which mounted whatever had been dropped there —
    # the WOOP module answered unauthenticated calls for two years that way.
    EXT_MODULES: str = ""

    # Browser origins allowed to call the API, comma separated. Empty means none, which is the
    # case of the console: it is served from the same origin as the API and needs no CORS
    # header at all. `CORS(app)` used to reflect every origin back.
    CORS_ALLOWED_ORIGINS: str = ""

    # Shared secret the Onfleet platform must present on its delivery webhooks. Empty means the
    # webhook refuses the payload instead of forwarding it to the broker untrusted.
    ONFLEET_WEBHOOK_SECRET: str = ""

    # Redis
    REDIS_IP: str = ""

    # Output
    OUTPUT_FOLDER: str = ""
    OUTPUT_URL: str = ""

    # Allowed roots for /streamfile (comma separated). Preserve access to the
    # standard log directory; deployments can override this with other roots.
    # The rootpath of every "file-system" app is also allowed.
    STREAM_ALLOWED_ROOTS: str = "/logs"

    # PostgreSQL
    PG_LOGIN: str = ""
    PG_PASSWORD: str = ""
    PG_HOST: str = ""
    PG_PORT: str = "5432"
    PG_DATABASE: str = ""

    # Elasticsearch / OpenSearch
    ELK_URL: str = ""
    # Declared again: the compose files still provide `ELK_PORT`, and `nyx_rest_helper.py` reads it
    # (`http://<host>:<port>`). Without the field, pydantic ignored the variable and the worker died
    # at import with `AttributeError: 'Settings' object has no attribute 'ELK_PORT'` — the reason
    # that image had not been rebuilt in ten months. The API does not use it (the transport defaults
    # to 9200 on an http URL); the field is here so the two agree.
    ELK_PORT: str = "9200"
    ELK_SSL: bool = False
    ELK_LOGIN: str = ""
    ELK_PASSWORD: str = ""

    # AMQC (ActiveMQ / STOMP)
    AMQC_URL: str = ""
    AMQC_PORT: str = "61613"
    AMQC_LOGIN: str = ""
    AMQC_PASSWORD: str = ""

    # Google OAuth (optional)
    CLIENT_SECRET_FILE: str = ""

    # Grafana (optional)
    GRAFANA_URL: str = ""
    GRAFANA_API_KEY: str = ""

    # SMTP (helper service)
    SMTP_ADDRESS: str = ""
    SMTP_USER: str = ""
    SMTP_PASSWORD: str = ""
    SMTP_FROM: str = ""
    SMTP_PORT: int = 25
    SMTP_SSL: bool = False
    SMTP_TLS: bool = False

    # SQL Server
    SQLSERVER_HOST: str = "NA"
    SQLSERVER_LOGIN: str = "NA"
    SQLSERVER_PASSWORD: str = "NA"
    SQLSERVER_PORT: int = 1433

    # Onfleet
    ONFLEET_APIKEY: str = ""

    # GitHub sign-in (OAuth app). All three are required to enable the button: the console asks
    # `GET /api/v1/cred/oauth/state` for the URL, so the client id never reaches the bundle.
    GITHUB_CLIENT_ID: str = ""
    GITHUB_CLIENT_SECRET: str = ""
    # The URL registered on the GitHub OAuth app — the console's `/oauth` page, absolute.
    GITHUB_REDIRECT_URI: str = ""

    # What to do with a GitHub identity that has no `nyx_user` account.
    # `refuse` (the default) turns it away: GitHub says who you are, `nyx_user` says what you may
    # open, and an unknown identity has nothing to open. `inactive` files a request instead — the
    # account is created **disabled and without privileges**, so the person lands on the page that
    # explains it and an administrator decides whether to activate it and what to grant. Any other
    # value is read as `refuse`: a typo must not open a door.
    GITHUB_UNKNOWN_ACCOUNTS: str = "refuse"

    # Microsoft Entra ID sign-in (OIDC, one directory). `ENTRA_TENANT_ID` is the **directory id**
    # (the GUID the portal shows as "Directory (tenant) ID"), not the domain name: every claim of
    # the identity token is checked against it, and a name would leave that check with nothing to
    # compare. A value that is not a GUID is read as "not configured", so the button stays hidden.
    # The redirect URI is registered on the application as a **Web** platform — the code is
    # exchanged server-side, which needs the client secret.
    ENTRA_TENANT_ID: str = ""
    ENTRA_CLIENT_ID: str = ""
    ENTRA_CLIENT_SECRET: str = ""
    ENTRA_REDIRECT_URI: str = ""
    # Same meaning as `GITHUB_UNKNOWN_ACCOUNTS`, for identities that arrive from Entra.
    ENTRA_UNKNOWN_ACCOUNTS: str = "refuse"

    # Kept for the (now removed) GitHub token exchange and for any JWT a client may ask for; the
    # sign-in itself mints a session token, not a JWT.
    OAUTH_CLIENT_SECRET: str = ""
    JWT_SECRET: str = ""


settings = Settings()
