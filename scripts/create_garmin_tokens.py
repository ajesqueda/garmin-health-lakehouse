"""One-time, run on your own computer: log in to Garmin and save reusable tokens.

    python scripts/create_garmin_tokens.py

It asks for your Garmin email, password and (if enabled) MFA code in the
terminal. The password is used once for this login and never saved. What gets
saved is the token file below (owner-only permissions), which ingestion uses
from then on. Store its contents in a Databricks secret; see docs/setup.md.

Treat the token file like a password: it grants access to your Garmin account
until you revoke it by logging out of Garmin Connect sessions.
"""

from __future__ import annotations

import getpass
from pathlib import Path

TOKEN_FILE = Path("~/.garminconnect/garmin_tokens.json")


def main() -> None:
    from garminconnect import Garmin

    email = input("Garmin email: ").strip()
    password = getpass.getpass("Garmin password (not saved): ")
    client = Garmin(email, password, prompt_mfa=lambda: input("MFA code: ").strip())
    client.login()
    # dump() writes with 0600 permissions inside a 0700 directory.
    client.client.dump(str(TOKEN_FILE))
    print(f"Saved tokens to {TOKEN_FILE.expanduser()}")
    print("Next: put them in a Databricks secret (docs/setup.md, step 3).")


if __name__ == "__main__":
    main()
