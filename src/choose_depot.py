from pathlib import Path

# import logging
import argparse

from P4 import P4, P4Exception

# # Setup Logging
# LOGFILE = "s3_migration.log"
# logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)
# file_handler = logging.FileHandler(LOGFILE)
# file_handler.setLevel(logging.DEBUG)  # logging level for the file handler
# stream_handler = logging.StreamHandler()
# stream_handler.setLevel(logging.INFO)  # logging level for the console handler
# formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
# file_handler.setFormatter(formatter)
# stream_handler.setFormatter(formatter)
# logger.addHandler(file_handler)
# logger.addHandler(stream_handler)


def main(p4):
    depot_root = get_p4_root_dir(p4)
    depot_names = get_p4_depots(p4)
    # Select a depot
    selected_depot = select_depot(depot_names)
    depot_dir = get_depot_dir(p4, selected_depot, depot_root)
    print(f"Depot {selected_depot} is located at {depot_dir}")


def get_depot_dir(p4, depot_name, depot_root) -> Path:
    depot_info = p4.run("depot", "-o", depot_name)[0]
    if "Address" in depot_info and "s3" in depot_info["Address"]:
        print(
            f"Warning, depot {depot_name} already has an s3 address of {depot_info['Address']}.\nBe sure you want to copy the files from the local disk to S3 since they may already be in S3."
        )
    # Check if depot has an absolute path in its spec
    if (
        Path(depot_info["Map"]).is_absolute()
        and Path(depot_info["Map"]).parent.exists()
    ):
        return Path(depot_info["Map"])
    # Otherwise, combine depot root and the relative dir
    elif (Path(depot_root) / Path(depot_info["Map"])).is_absolute() and (
        Path(depot_root) / Path(depot_info["Map"])
    ).parent.exists():
        return Path(depot_root) / Path(depot_info["Map"])
    # If that path doesn't exist, raise an error.
    else:
        print(
            f"Depot {depot_name} does not have a local directory that exists at {Path(depot_root) / Path(depot_info['Map'])}. Be sure to run this script on the server itself, as the same user as the p4d service."
        )
        raise FileExistsError(
            f"Depot {depot_name} does not have a local directory that exists at {Path(depot_root) / Path(depot_info['Map'])}. Be sure to run this script on the server itself, as the same user as the p4d service."
        )


def get_p4_root_dir(p4) -> Path:
    """
    Returns the local directory where depots are stored.
    This is either the P4ROOT environment variable or the server.depot.root configuration value if it is set.
    """
    p4root = p4.run("configure", "show", "P4ROOT")[0]["Value"]
    depot_root = p4.run("configure", "show", "server.depot.root")
    depot_root = Path(depot_root[0]["Value"] if depot_root else p4root)
    return depot_root


def get_p4_depots(p4) -> list[str]:
    """
    Returns a list of all depots in the Perforce server.
    """
    return [
        d["name"]
        for d in p4.run("depots")
        if d["type"] in ["stream", "local", "archive"]
    ]


def select_depot(depot_names):
    print("Available depots:")
    for i, name in enumerate(depot_names, 1):
        print(f"{i}. {name}")

    while True:
        try:
            choice = int(input("\nEnter the number of the depot you want to select: "))
            if 1 <= choice <= len(depot_names):
                return depot_names[choice - 1]
            else:
                print("Invalid choice. Please try again.")
        except ValueError:
            print("Invalid input. Please enter a number.")


def connect_to_p4(port=None, user=None, passwd=None):
    p4 = P4()
    try:
        p4.port = port or p4.port
        p4.user = user or p4.user
        p4.password = passwd or p4.password
        print(f"Connecting to {p4.port} as {p4.user}")
        p4.connect()
    except Exception:
        print(f"Could not connect to Perforce server at {p4.port} as {p4.user}")
        raise
    return p4


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Interactively select a depot to migrate to s3"
    )
    parser.add_argument(
        "-p",
        "--port",
        help="Overrides any P4PORT setting with the specified protocol:host:port.",
    )
    parser.add_argument(
        "-P",
        "--password",
        help="Enables a password (or ticket) to be passed on the command line, thus bypassing the password associated with P4PASSWD.",
    )
    parser.add_argument(
        "-u",
        "--user",
        help="Overrides any P4USER setting with the specified user name.",
    )

    args = parser.parse_args()
    # Connect to Perforce server
    p4 = connect_to_p4(args.port, args.user, args.password)
    main(p4)
