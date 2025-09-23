# role_mapper.py
import os, json
from dotenv import load_dotenv

load_dotenv()

GROUP_ROLE_MAP = json.loads(os.getenv("GROUP_ROLE_MAP", "{}"))

def extract_roles_from_ad(user_info, mapping=GROUP_ROLE_MAP):
    """Convert AD groups (memberOf) to internal app roles."""
    roles = []
    groups = user_info.get("memberOf", [])

    for dn in groups:
        cn = dn.split(",")[0].replace("CN=", "")
        if cn in mapping:
            roles.append(mapping[cn])
    return roles
