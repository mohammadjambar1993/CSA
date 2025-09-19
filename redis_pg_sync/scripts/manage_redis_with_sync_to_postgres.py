import argparse
import logging
import os
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple, Union

try:
    from colorama import Fore, Style, init
    from dotenv import load_dotenv
except ImportError:
    print("Required packages not found. Please install them with:")
    print("pip install colorama python-dotenv")
    sys.exit(1)

init(autoreset=True)

SCRIPT_DIR = Path(__file__).parent.resolve()
ROOT_DIR = SCRIPT_DIR.parent
DOCKER_DIR = ROOT_DIR / "docker"
ENV_CONF = ROOT_DIR / "env.conf"
LOG_FILE = SCRIPT_DIR / "redis_with_sync_to_postgres.log"

# Setup logging with consistent format
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)


class ScriptError(Exception):
    """Base exception class for script errors."""

    pass


class ConfigurationError(ScriptError):
    """Error in configuration files or environment."""

    pass


class DockerError(ScriptError):
    """Error related to Docker operations."""

    pass


class ValidationError(ScriptError):
    """Error in input validation."""

    pass


def validate_service_name(service_name: str) -> bool:
    """
    Validate that a service name contains only allowed characters
    to prevent command injection.
    """
    if not service_name or not isinstance(service_name, str):
        return False
    # Only allow alphanumeric chars, underscores, and dashes
    return bool(re.match(r"^[a-zA-Z0-9_-]+$", service_name))


def printc(msg, color, level=logging.INFO):
    """Print colored message and log it with specified level."""
    print(color + msg + Style.RESET_ALL)
    if level == logging.ERROR:
        logging.error(msg)
    elif level == logging.WARNING:
        logging.warning(msg)
    else:
        logging.info(msg)


def load_env_conf():
    """Load environment variables from env.conf file using dotenv."""
    env = os.environ.copy()
    try:
        if ENV_CONF.exists():
            # Load environment variables from the env.conf file
            load_dotenv(ENV_CONF)
            # Update our copy of the environment with the loaded variables
            for key, value in os.environ.items():
                env[key] = value
        return env
    except Exception as e:
        raise ConfigurationError(f"Failed to load environment configuration: {e}")


def run_command(cmd: List[str], cwd=None, env=None, check=True) -> Tuple[int, str, str]:
    """
    Run a command and return the return code, stdout, and stderr.
    Centralizes command execution.

    All arguments must be provided as a list to avoid shell injection.
    Never use shell=True for security reasons.
    """
    try:
        # Ensure all command elements are strings
        cmd = [str(arg) for arg in cmd]

        result = subprocess.run(
            cmd,
            cwd=cwd,
            env=env,
            capture_output=True,
            text=True,
            check=check,
            shell=False,  # Explicitly set shell=False for security
        )
        return result.returncode, result.stdout, result.stderr
    except subprocess.CalledProcessError as e:
        # Return the error information for handling by the caller
        return e.returncode, e.stdout, e.stderr
    except Exception as e:
        # For other exceptions, return error codes and message
        return 1, "", str(e)


def check_docker():
    """Check if Docker is running."""
    returncode, stdout, stderr = run_command(["docker", "info"], check=False)
    if returncode != 0:
        printc(
            "Error: Docker is not running. Please start Docker first.",
            Fore.RED,
            logging.ERROR,
        )
        return False
    return True


def check_docker_compose():
    """Check if docker-compose is installed."""
    returncode, stdout, stderr = run_command(
        ["docker-compose", "--version"], check=False
    )
    if returncode != 0:
        printc("Error: docker-compose is not installed.", Fore.RED, logging.ERROR)
        return False
    return True


def run_compose_command(
    command_args: List[str], services: Union[List[str], None] = None
):
    """Runs a docker-compose command, optionally targeting specific services."""
    if not check_docker() or not check_docker_compose():
        return None

    # Assuming compose file is in DOCKER_DIR (e.g., ../docker/docker-compose.yml)
    compose_file_path = DOCKER_DIR / "docker-compose.yml"
    if not compose_file_path.exists():
        printc(
            f"Error: Docker Compose file not found at {compose_file_path}",
            Fore.RED,
            logging.ERROR,
        )
        return None

    # Validate all command arguments to prevent injection
    for arg in command_args:
        if not isinstance(arg, str):
            printc(f"Error: Invalid command argument: {arg}", Fore.RED, logging.ERROR)
            return None

    # Validate service names if provided
    if services:
        for service in services:
            if not validate_service_name(service):
                printc(
                    f"Error: Invalid service name '{service}'. Only alphanumeric characters, dashes, and underscores are allowed.",
                    Fore.RED,
                    logging.ERROR,
                )
                return None

    base_command = ["docker-compose", "-f", str(compose_file_path)]
    action = command_args[0]
    options = command_args[1:]

    full_command = base_command + [action] + options
    if services and action == "up":
        full_command.extend(services)

    printc(f"Running command: {' '.join(full_command)}", Fore.CYAN)

    try:
        env = load_env_conf()
        returncode, stdout, stderr = run_command(full_command, cwd=DOCKER_DIR, env=env)

        if returncode != 0:
            printc(f"Command failed with error: {stderr}", Fore.RED, logging.ERROR)
            return None

        logging.info(stdout)
        if stderr:
            logging.warning(stderr)

        if action == "up":
            printc("Services started successfully!", Fore.GREEN)
            printc("Selected services are running in the background.", Fore.GREEN)
            printc(
                f"To view logs: docker-compose -f {compose_file_path} logs -f {' '.join(services or [])}",
                Fore.YELLOW,
            )
            printc(f"To stop services: python {Path(__file__).name} stop", Fore.YELLOW)

        # Display service status
        printc("\nService Status:", Fore.GREEN)
        returncode, status_output, stderr = run_command(
            ["docker-compose", "ps"], cwd=DOCKER_DIR, env=env
        )
        if returncode == 0:
            print(status_output)
            logging.info(status_output)
        else:
            printc(
                "Warning: Could not retrieve service status.",
                Fore.YELLOW,
                logging.WARNING,
            )

        return stdout
    except Exception as e:
        printc(f"Error executing docker-compose command: {e}", Fore.RED, logging.ERROR)
        return None


def start_services(services_to_start: Optional[List[str]] = None):
    """Start Redis-Postgres Synchronization Service."""
    printc("Starting Redis-Postgres Synchronization Service", Fore.GREEN)

    if not ENV_CONF.exists():
        printc(
            "Warning: env.conf file not found. Using default configuration.",
            Fore.YELLOW,
            logging.WARNING,
        )

    try:
        # Validate services if specified
        if services_to_start:
            for service in services_to_start:
                if not validate_service_name(service):
                    printc(
                        f"Error: Invalid service name '{service}'. Only alphanumeric characters, dashes, and underscores are allowed.",
                        Fore.RED,
                        logging.ERROR,
                    )
                    return False

        # Assuming standard services: sync app, local redis, local postgres
        # Confirm these service names match your docker-compose.yml
        default_services = [
            "redis-cache",
            "redis-postgres-sync-service",
            "redis-stream-processor-service",
        ]
        if services_to_start is None:
            services_to_start = default_services
            printc(
                f"Starting default services: {', '.join(services_to_start)}...",
                Fore.GREEN,
            )
        else:
            # Ensure the main service is always included
            if "redis-postgres-sync-service" not in services_to_start:
                printc(
                    "Warning: 'redis-postgres-sync-service' service not specified, adding it.",
                    Fore.YELLOW,
                    logging.WARNING,
                )
                services_to_start.append("redis-postgres-sync-service")
            printc(
                f"Starting specified services: {', '.join(services_to_start)}...",
                Fore.GREEN,
            )

        printc("Building and starting services...", Fore.GREEN)
        result = run_compose_command(
            ["up", "--build", "-d"], services=services_to_start
        )

        if result is None:
            printc("Failed to start services.", Fore.RED, logging.ERROR)
            return False

        return True
    except Exception as e:
        printc(f"Error starting services: {e}", Fore.RED, logging.ERROR)
        return False


def stop_services():
    """Stop Redis-Postgres Synchronization Services."""
    printc("Stopping Redis-Postgres Synchronization Services...", Fore.YELLOW)
    compose_file_path = DOCKER_DIR / "docker-compose.yml"

    if not compose_file_path.exists():
        printc(
            f"Warning: Docker Compose file not found at {compose_file_path}. Cannot guarantee proper shutdown.",
            Fore.YELLOW,
            logging.WARNING,
        )
        return False

    try:
        result = run_compose_command(["down"])
        if result is None:
            printc("Error stopping services.", Fore.RED, logging.ERROR)
            return False

        printc("Services stopped successfully.", Fore.GREEN)

        # Optional: Clean up Docker volumes
        try:
            response = input(
                "Do you want to remove all related Docker volumes? (y/N): "
            )
        except EOFError:
            response = "n"

        if response.strip().lower() in ["y", "yes"]:
            printc("Removing Docker volumes...", Fore.YELLOW)
            try:
                # Only remove volumes matching postgres_sync_redis-data
                returncode, stdout, stderr = run_command(
                    ["docker", "volume", "ls", "-q"]
                )
                if returncode == 0:
                    volumes = stdout.splitlines()
                    removed_any = False
                    for v in volumes:
                        # Strict validation of volume name before removal
                        if "redis_pg_sync_redis-data" in v and re.match(
                            r"^[a-zA-Z0-9_-]+$", v
                        ):
                            returncode, stdout, stderr = run_command(
                                ["docker", "volume", "rm", v]
                            )
                            if returncode == 0:
                                printc(f"Removed volume: {v}", Fore.GREEN)
                                removed_any = True
                            else:
                                printc(
                                    f"Failed to remove volume {v}: {stderr}",
                                    Fore.YELLOW,
                                    logging.WARNING,
                                )

                    if not removed_any:
                        printc("No matching volumes found to remove.", Fore.YELLOW)
                else:
                    printc(
                        f"Could not list volumes: {stderr}",
                        Fore.YELLOW,
                        logging.WARNING,
                    )
            except Exception as e:
                printc(f"Error removing volumes: {e}", Fore.RED, logging.ERROR)
            printc("Volumes removed.", Fore.GREEN)
        printc("All services have been stopped.", Fore.GREEN)
        return True
    except Exception as e:
        printc(f"Error stopping services: {e}", Fore.RED, logging.ERROR)
        return False


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description="Manage Redis-Postgres Sync Service")
    subparsers = parser.add_subparsers(dest="action", help="Action to perform")

    # Start command
    start_parser = subparsers.add_parser(
        "start", help="Build and start services with Docker Compose."
    )
    start_parser.add_argument(
        "--services",
        nargs="+",
        help="Specify which services to start. Defaults to all services defined in docker-compose.yml.",
        choices=[
            "redis-cache",
            "redis-postgres-sync-service",
            "redis-stream-processor-service",
        ],
        metavar="SERVICE",
    )

    # Stop command
    stop_parser = subparsers.add_parser(
        "stop", help="Stop and remove services defined in Docker Compose."
    )

    args = parser.parse_args()

    if not args.action:
        parser.print_help()
        return 0

    # Check for docker dir existence, compose file checked in run_compose_command/stop_services
    if not DOCKER_DIR.exists():
        printc(
            f"Error: Cannot find Docker directory at {DOCKER_DIR}",
            Fore.RED,
            logging.ERROR,
        )
        return 1

    try:
        if args.action == "start":
            # Validate services if provided
            if args.services:
                for service in args.services:
                    if not validate_service_name(service):
                        printc(
                            f"Error: Invalid service name '{service}'. Only alphanumeric characters, dashes, and underscores are allowed.",
                            Fore.RED,
                            logging.ERROR,
                        )
                        return 1
            # Services are already validated through argparse choices
            success = start_services(args.services)
        elif args.action == "stop":
            success = stop_services()
        else:
            parser.print_help()
            return 0

        return 0 if success else 1
    except Exception as e:
        printc(f"Unhandled error: {e}", Fore.RED, logging.ERROR)
        return 1


if __name__ == "__main__":
    sys.exit(main())
