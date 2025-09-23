from ldap3 import Server, Connection, ALL, NTLM, ALL_ATTRIBUTES, ALL_OPERATIONAL_ATTRIBUTES
import logging
import os
from dotenv import load_dotenv
load_dotenv()

logger = logging.getLogger()

AD_SERVERS = os.getenv("AD_SERVER", "").split(',')
AD_DOMAIN = os.getenv("AD_DOMAIN")
AD_BASE_DN = os.getenv("AD_BASE_DN")

def authenticate_ad(username, password):
    """ Authenticate user against Active Directory and return all user attributes. 
        Returns: (bool_success, user_info_dict_or_empty)
    """
    if not AD_BASE_DN or not AD_DOMAIN or not AD_SERVERS:
        logger.error("Active Directory environment variables are not set properly.")
        return False, {}

    # normalize username for bind and for search filter
    # support UPN (user@domain) and sAMAccountName (user)
    if "@" in username:
        username_without_domain = username.split("@")[0]
        bind_user = f"{AD_DOMAIN}\\{username_without_domain}"  # use UPN for bind
        search_filter = f'(sAMAccountName={username_without_domain})'
    else:
        # use DOMAIN\user for bind
        bind_user = f"{AD_DOMAIN}\\{username}"
        search_filter = f'(sAMAccountName={username})'

    for server_url in AD_SERVERS:
        try:
            server = Server(server_url, get_info=ALL)
            conn = Connection(
                server,
                user=bind_user,
                password=password,
                authentication=NTLM,
                auto_bind=True
            )

            if conn.bound:
                # Request all user attributes (regular + operational)
                # You can use ALL_ATTRIBUTES / ALL_OPERATIONAL_ATTRIBUTES or ['*','+']
                success = conn.search(
                    search_base=AD_BASE_DN,
                    search_filter=search_filter,
                    search_scope='SUBTREE',
                    attributes=[ALL_ATTRIBUTES, ALL_OPERATIONAL_ATTRIBUTES]
                )
                if success and conn.entries:
                    # entry_attributes_as_dict returns a dict of attribute -> list/values
                    user_info = conn.entries[0].entry_attributes_as_dict
                    # optionally include the DN
                    user_info['distinguishedName'] = str(conn.entries[0].entry_dn)
                    conn.unbind()
                    return True, user_info
                else:
                    logger.warning(f"No entries found for filter {search_filter} on {server_url}")
                    conn.unbind()
                    return True, {}  # auth ok but no extra info found

        except Exception as e:
            logger.error(f"Error connecting to AD server {server_url}: {e}", exc_info=True)
            continue

    return False, {}