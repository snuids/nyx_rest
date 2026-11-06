"""Which destinations `/sendmessage` may publish to.

The route forwarded any destination the caller supplied. Publication is the platform's internal
bus, so an account holding nothing more than an ordinary token could write on the queues the
workers consume — `/queue/REST_LOAD_DATA` in particular, which exports whatever index the message
names and mails it to the address the message names too. Publishing to an unknown topic was worse
still: any consumer that trusts its inputs reads it as a platform event.

The interface only ever publishes to the destinations below; the free-form destination field of
the administration screen keeps working, but for administrators only.
"""

LOAD_DATA = "/queue/REST_LOAD_DATA"
LOAD_PGDATA = "/queue/REST_LOAD_PGDATA"
LOAD_KIBANA = "/queue/REST_LOAD_KIBANA"
REPORTRUNNER_COMMAND = "/topic/NYX_REPORTRUNNER_COMMAND"
LAMBDA_COMMAND = "/topic/NYX_LAMBDA_COMMAND"

# What the interface itself publishes. Anything else needs the administration privilege.
PLATFORM_DESTINATIONS = (
    LOAD_DATA,
    LOAD_PGDATA,
    LOAD_KIBANA,
    REPORTRUNNER_COMMAND,
    LAMBDA_COMMAND,
)

ADMIN_PRIVILEGE = "admin"


def is_admin(user):
    return ADMIN_PRIVILEGE in ((user or {}).get("privileges") or [])


def is_allowed_destination(destination, user):
    """Administrators publish anywhere; everyone else only to the platform's own destinations."""
    target = str(destination or "").strip()
    if not target:
        return False
    if is_admin(user):
        return True
    return target in PLATFORM_DESTINATIONS
