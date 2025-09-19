import argparse
import logging
import os
import subprocess
import sys
from pathlib import Path
from typing import List, Optional  # Added for type hinting

try:
    from colorama import Fore, Style, init
except ImportError:
    print("colorama not found. Please install it with 'pip install colorama'")
    sys.exit(1)

init(autoreset=True)

SCRIPT_DIR = Path(__file__).parent.resolve()
ROOT_DIR = SCRIPT_DIR.parent
DOCKER_DIR = ROOT_DIR / "docker"
ENV_CONF = ROOT_DIR / "env.conf"
COMPOSE_FILE = DOCKER_DIR / "docker-compose.yml"
LOG_FILE = SCRIPT_DIR / "redis_redis_manager.log"

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)


def printc(msg, color):
    print(color + msg + Style.RESET_ALL)
    logging.info(msg)


def load_env_conf():
    env = os.environ.copy()
    if ENV_CONF.exists():
        with open(ENV_CONF) as f:
            for line in f:
                if line.strip() and not line.strip().startswith("#") and "=" in line:
                    k, v = line.strip().split("=", 1)
                    env[k] = v
    return env


def check_docker():
    try:
        subprocess.run(
            ["docker", "info"],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        return True
    except Exception as e:
        printc("Error: Docker is not running. Please start Docker first.", Fore.RED)
        logging.error(str(e))
        return False


def check_docker_compose():
    try:
        subprocess.run(
            ["docker-compose", "--version"],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        return True
    except Exception as e:
        printc("Error: docker-compose is not installed.", Fore.RED)
        logging.error(str(e))
        return False


def run_compose_command(command_args: List[str], services: List[str] = None):
    """Runs a docker-compose command, optionally targeting specific services."""
    if not check_docker() or not check_docker_compose():
        sys.exit(1)

    base_command = ["docker-compose", "-f", str(COMPOSE_FILE)]
    action = command_args[0]  # e.g., 'up', 'down', 'ps'
    options = command_args[1:]  # e.g., ['--build', '-d']

    # Construct the final command
    full_command = base_command + [action] + options
    if services:
        # Append service names if provided (typically for 'up')
        full_command.extend(services)
    elif action == "ps":
        # 'ps' should show all by default, but can optionally filter
        pass
    elif action == "down":
        # 'down' affects all services defined in the compose file by default
        pass

    printc(f"Running command: {' '.join(full_command)}", Fore.CYAN)

    env = load_env_conf()
    result = subprocess.run(
        full_command,
        cwd=DOCKER_DIR,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    output = result.stdout.decode()
    logging.info(output)

    if result.returncode != 0:
        printc(
            f"Error executing docker-compose command: {' '.join(command_args)}",
            Fore.RED,
        )
        print(output)
        sys.exit(1)
    else:
        printc(f"Command executed successfully: {' '.join(command_args)}", Fore.GREEN)
        if command_args[0] in ["up", "down"]:
            printc("\nCurrent Service Status:", Fore.GREEN)
            # Run 'ps' without service list to show status of all relevant services
            status_result = subprocess.run(
                base_command + ["ps"],
                cwd=DOCKER_DIR,
                env=env,
                capture_output=True,
                text=True,
            )
            print(status_result.stdout)
            logging.info(status_result.stdout)
    return output


def start_services(services_to_start: Optional[List[str]] = None):
    printc("Starting Redis-to-Redis Synchronization Service", Fore.GREEN)
    if not ENV_CONF.exists():
        printc(
            "Warning: env.conf file not found. Using default configuration.",
            Fore.YELLOW,
        )

    default_services = ["redis-to-redis-sync", "source-redis", "target-redis"]
    if services_to_start is None:
        services_to_start = default_services
        printc(
            f"Starting default services: {', '.join(services_to_start)}...", Fore.GREEN
        )
    else:
        # Ensure the main service is always included if specific services are requested
        if "redis-to-redis-sync" not in services_to_start:
            printc(
                "Warning: 'redis-to-redis-sync' service not specified, adding it to the start list.",
                Fore.YELLOW,
            )
            services_to_start.append("redis-to-redis-sync")
        printc(
            f"Starting specified services: {', '.join(services_to_start)}...",
            Fore.GREEN,
        )

    printc("Building and starting services...", Fore.GREEN)
    run_compose_command(["up", "--build", "-d"], services=services_to_start)
    printc("Services started successfully!", Fore.GREEN)
    printc("Selected services are running in the background.", Fore.GREEN)
    printc(
        f"To view logs: docker-compose -f {COMPOSE_FILE} logs -f {' '.join(services_to_start)}",
        Fore.YELLOW,
    )
    printc(f"To stop services: python {Path(__file__).name} stop", Fore.YELLOW)


def stop_services():
    printc("Stopping Redis-to-Redis Synchronization Services...", Fore.YELLOW)
    # 'down' affects all services managed by the compose file
    run_compose_command(["down"])
    printc("Services stopped successfully.", Fore.GREEN)

    try:
        response = input("Do you want to remove all related Docker volumes? (y/N): ")
    except EOFError:
        response = "n"
    if response.strip().lower() in ["y", "yes"]:
        printc("Removing Docker volumes...", Fore.YELLOW)
        # Specify volumes associated with this compose file if possible
        run_compose_command(["down", "--volumes"])
        printc("Volumes removed.", Fore.GREEN)
    printc("All services have been stopped.", Fore.GREEN)


def main():
    parser = argparse.ArgumentParser(description="Manage Redis-to-Redis Sync Services")
    subparsers = parser.add_subparsers(dest="action", help="Action to perform")

    # Start command
    start_parser = subparsers.add_parser(
        "start", help="Build and start services with Docker Compose."
    )
    start_parser.add_argument(
        "--services",
        nargs="+",
        help="Specify which services to start (e.g., redis-to-redis-sync source-redis). Defaults to all: redis-to-redis-sync, source-redis, target-redis.",
        choices=["redis-to-redis-sync", "source-redis", "target-redis"],
        metavar="SERVICE",
    )

    # Stop command
    stop_parser = subparsers.add_parser(
        "stop", help="Stop and remove services defined in Docker Compose."
    )
    # No arguments needed for stop as 'down' affects all

    args = parser.parse_args()

    if not args.action:
        parser.print_help()
        sys.exit(0)

    if not DOCKER_DIR.exists() or not COMPOSE_FILE.exists():
        printc(f"Error: Cannot find Docker configuration at {DOCKER_DIR}", Fore.RED)
        sys.exit(1)

    if args.action == "start":
        start_services(args.services)  # Pass the list of services
    elif args.action == "stop":
        stop_services()


if __name__ == "__main__":
    main()
