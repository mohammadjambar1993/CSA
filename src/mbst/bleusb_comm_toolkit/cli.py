import argparse
import sys


def run_ble(device_id=None, user_id=None, nick_name=None):
    from mbst.examples.example_ble import run

    run(device_id=device_id, user_id=user_id, nick_name=nick_name)


def run_redis(device_id=None, user_id=None, nick_name=None):
    from mbst.examples.example_stream_to_redis import run

    if user_id is None:
        user_id = input("Enter user ID (username): ").strip()
    if nick_name is None:
        nick_name = input("Enter nick name: ").strip()
    run(device_id=device_id, user_id=user_id, nick_name=nick_name)


def run_redis_multiple(device_id=None, user_id=None, nick_name=None):
    from mbst.examples.example_redis_multiple_consumers import run

    if user_id is None:
        user_id = input("Enter user ID (username): ").strip()
    if nick_name is None:
        nick_name = input("Enter nick name: ").strip()
    run(device_id=device_id, user_id=user_id, nick_name=nick_name)


def run_consumer(device_id=None, user_id=None, nick_name=None):
    from mbst.examples.example_consumer import run

    run(device_id=device_id, user_id=user_id, nick_name=nick_name)


def get_version():
    try:
        from mbst.bleusb_comm_toolkit._version import version as version_str

        return version_str
    except ImportError:
        return "development"


def main():
    parser = argparse.ArgumentParser(
        description=f"MBST SDK v{get_version()}\n\nA Python SDK for BLE/USB data streaming with Redis integration",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=(
            "Author: Sarah Gulzar\n"
            "Email: sarah.gulzar@myant.ca\n"
            "\nExample usage:\n"
            "  mbst-sdk.exe ble\n"
            "  mbst-sdk.exe redis\n"
            "  mbst-sdk.exe consumer\n"
            "  mbst-sdk.exe redis-multiple\n"
        ),
    )
    parser.add_argument(
        "--version", action="version", version=f"%(prog)s v{get_version()}"
    )

    subparsers = parser.add_subparsers(dest="command", help="Choose a mode to run")

    for cmd in ["ble", "redis", "consumer", "redis-multiple"]:
        sub = subparsers.add_parser(cmd, help=f"Run with {cmd.upper()}")
        sub.add_argument("--device-id", type=str, help="Device ID (board address)")
        sub.add_argument("--user-id", type=str, help="User ID")
        sub.add_argument("--nick-name", type=str, help="User Nickname")

    subparsers.add_parser("start", help="Run the start script with user prompts")

    args = parser.parse_args()

    if args.command == "ble":
        run_ble(args.device_id, args.user_id, args.nick_name)
    elif args.command == "redis":
        run_redis(args.device_id, args.user_id, args.nick_name)
    elif args.command == "consumer":
        run_consumer(args.device_id, args.user_id, args.nick_name)
    elif args.command == "redis-multiple":
        run_redis_multiple(args.device_id, args.user_id, args.nick_name)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
