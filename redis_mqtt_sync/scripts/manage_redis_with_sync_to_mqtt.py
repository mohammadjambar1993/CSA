import argparse
import logging
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    from colorama import Fore, Style, init
    from dotenv import load_dotenv
except ImportError:
    print("Required packages not found. Please install dependencies with:")
    print(f"pip install -r {Path(__file__).parent.parent}/requirements.txt")
    sys.exit(1)

init(autoreset=True)

SCRIPT_DIR = Path(__file__).parent.resolve()
ROOT_DIR = SCRIPT_DIR.parent
DOCKER_DIR = ROOT_DIR / "docker"
ENV_CONF = ROOT_DIR / "env.conf"
COMPOSE_FILE = DOCKER_DIR / "docker-compose.yml"
LOG_FILE = SCRIPT_DIR / "manage_redis_with_sync_to_mqtt.log"

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


def printc(msg: str, color: str, level: int = logging.INFO) -> None:
    """Print colored message and log it with specified level."""
    print(color + msg + Style.RESET_ALL)
    if level == logging.ERROR:
        logging.error(msg)
    elif level == logging.WARNING:
        logging.warning(msg)
    else:
        logging.info(msg)


def load_env_conf() -> Dict[str, str]:
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
        else:
            printc(
                f"Warning: env.conf file not found at {ENV_CONF}",
                Fore.YELLOW,
                logging.WARNING,
            )
            printc("\nTroubleshooting steps:", Fore.YELLOW)
            printc(
                f"1. Create env.conf file: cp {ROOT_DIR}/env.conf.example {ENV_CONF}",
                Fore.YELLOW,
            )
            printc(
                "2. Edit env.conf with appropriate settings for your environment",
                Fore.YELLOW,
            )
            printc("3. Run this command again", Fore.YELLOW)
            raise ConfigurationError("env.conf file not found")
    except Exception as e:
        if "env.conf file not found" not in str(e):
            printc(
                f"Failed to load environment configuration: {e}",
                Fore.RED,
                logging.ERROR,
            )
            printc("\nTroubleshooting steps:", Fore.YELLOW)
            printc("1. Check that env.conf has proper formatting", Fore.YELLOW)
            printc(
                "2. Ensure no syntax errors in the configuration values", Fore.YELLOW
            )
            printc("3. Verify that required variables are defined:", Fore.YELLOW)
            printc("   - REDIS_HOST, REDIS_PORT", Fore.YELLOW)
            printc(
                "   - MQTT_HOST, MQTT_PORT, MQTT_USERNAME, MQTT_PASSWORD", Fore.YELLOW
            )
        raise ConfigurationError(f"Failed to load environment configuration: {e}")


def run_command(
    cmd: List[str],
    cwd: Optional[Path] = None,
    env: Optional[Dict[str, str]] = None,
    check: bool = True,
) -> Tuple[int, str, str]:
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


def check_docker() -> bool:
    """Check if Docker is running."""
    returncode, stdout, stderr = run_command(["docker", "info"], check=False)
    if returncode != 0:
        printc("Error: Docker is not running.", Fore.RED, logging.ERROR)
        printc("\nTroubleshooting steps:", Fore.YELLOW)
        printc(
            "1. Start Docker Desktop application if you're on Windows/Mac", Fore.YELLOW
        )
        printc("2. On Linux, try: sudo systemctl start docker", Fore.YELLOW)
        printc("3. Verify Docker is running: docker info", Fore.YELLOW)
        printc("4. Once Docker is running, try this command again", Fore.YELLOW)
        return False
    return True


def check_docker_compose() -> bool:
    """Check if docker-compose is installed."""
    returncode, stdout, stderr = run_command(
        ["docker-compose", "--version"], check=False
    )
    if returncode != 0:
        printc("Error: docker-compose is not installed.", Fore.RED, logging.ERROR)
        printc("\nTroubleshooting steps:", Fore.YELLOW)
        printc("1. Install Docker Compose:", Fore.YELLOW)
        printc(
            "   - On most systems, Docker Desktop includes Docker Compose", Fore.YELLOW
        )
        printc(
            "   - For Linux: sudo apt install docker-compose or sudo yum install docker-compose",
            Fore.YELLOW,
        )
        printc("2. Verify installation: docker-compose --version", Fore.YELLOW)
        return False
    return True


def validate_service_name(service_name: str) -> bool:
    """
    Validate that a service name contains only allowed characters
    to prevent command injection.
    """
    if not service_name or not isinstance(service_name, str):
        return False
    # Only allow alphanumeric chars, underscores, and dashes
    return bool(re.match(r"^[a-zA-Z0-9_-]+$", service_name))


def run_compose_command(
    command_args: List[str], services: Optional[List[str]] = None
) -> Optional[str]:
    """Runs a docker-compose command, optionally targeting specific services."""
    if not check_docker() or not check_docker_compose():
        return None

    if not COMPOSE_FILE.exists():
        printc(
            f"Error: Docker Compose file not found at {COMPOSE_FILE}",
            Fore.RED,
            logging.ERROR,
        )
        printc("\nTroubleshooting steps:", Fore.YELLOW)
        printc(f"1. Check that the docker directory exists: {DOCKER_DIR}", Fore.YELLOW)
        printc(
            "2. Verify that docker-compose.yml exists in that directory", Fore.YELLOW
        )
        printc(
            "3. If missing, re-clone the repository or restore the file", Fore.YELLOW
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

    base_command = ["docker-compose", "-f", str(COMPOSE_FILE)]
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

        # Print status for up/down commands
        if action in ["up", "down"]:
            printc("\nCurrent Service Status:", Fore.GREEN)
            returncode, status_output, stderr = run_command(
                base_command + ["ps"], cwd=DOCKER_DIR, env=env
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


def start_services(services_to_start: Optional[List[str]] = None) -> bool:
    """Start Redis-MQTT Synchronization Service."""
    printc("Starting Redis-MQTT Synchronization Service", Fore.GREEN)

    if not ENV_CONF.exists():
        printc("Warning: env.conf file not found.", Fore.YELLOW, logging.WARNING)
        printc(
            "Copy env.conf.example to env.conf and modify it for your environment.",
            Fore.YELLOW,
            logging.WARNING,
        )

        # Prompt user if they want to create a default env.conf
        try:
            response = input("Do you want to create a default env.conf now? (y/N): ")
            if response.strip().lower() in ["y", "yes"]:
                # Copy example config to env.conf
                example_conf = ROOT_DIR / "env.conf.example"
                if example_conf.exists():
                    import shutil

                    shutil.copy(example_conf, ENV_CONF)
                    printc(
                        f"Created {ENV_CONF} from example. Please edit it with your settings.",
                        Fore.GREEN,
                    )
                    printc(
                        "You will need to run this command again after editing.",
                        Fore.YELLOW,
                    )
                else:
                    printc(
                        "Error: env.conf.example not found.", Fore.RED, logging.ERROR
                    )
            return False
        except EOFError:
            printc("No input received. Please create env.conf manually.", Fore.YELLOW)
            return False

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

        # Default services include only the sync app and redis
        # No longer include mqtt-broker as it should be remote
        default_services = ["redis-mqtt-sync", "redis"]

        if services_to_start is None:
            services_to_start = default_services
            printc(
                f"Starting default services: {', '.join(services_to_start)}...",
                Fore.GREEN,
            )
        else:
            # Ensure the main service is always included
            if "redis-mqtt-sync" not in services_to_start:
                printc(
                    "Warning: 'redis-mqtt-sync' service not specified, adding it.",
                    Fore.YELLOW,
                    logging.WARNING,
                )
                services_to_start.append("redis-mqtt-sync")
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

        printc("Services started successfully!", Fore.GREEN)
        printc("Selected services are running in the background.", Fore.GREEN)
        printc(
            f"To view logs: docker-compose -f {COMPOSE_FILE} logs -f {' '.join(services_to_start)}",
            Fore.YELLOW,
        )
        printc(f"To stop services: python {Path(__file__).name} stop", Fore.YELLOW)

        # Print remote MQTT configuration if available
        env = load_env_conf()
        mqtt_host = env.get("MQTT_HOST", "not configured")
        mqtt_port = env.get("MQTT_PORT", "1883")
        mqtt_topic = env.get("MQTT_BASE_TOPIC", "redis/stream")
        printc(
            f"\nConnecting to remote MQTT broker: {mqtt_host}:{mqtt_port}", Fore.YELLOW
        )
        printc(f"Using MQTT base topic: {mqtt_topic}", Fore.YELLOW)

        return True
    except Exception as e:
        printc(f"Error starting services: {e}", Fore.RED, logging.ERROR)
        return False


def stop_services() -> bool:
    """Stop Redis-MQTT Synchronization Services."""
    printc("Stopping Redis-MQTT Synchronization Services...", Fore.YELLOW)

    if not COMPOSE_FILE.exists():
        printc(
            f"Warning: Docker Compose file not found at {COMPOSE_FILE}. Cannot guarantee proper shutdown.",
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

        try:
            response = input(
                "Do you want to remove all related Docker volumes? (y/N): "
            )
        except EOFError:
            response = "n"

        if response.strip().lower() in ["y", "yes"]:
            printc("Removing Docker volumes...", Fore.YELLOW)
            result = run_compose_command(["down", "--volumes"])
            if result is not None:
                printc("Volumes removed.", Fore.GREEN)
            else:
                printc("Error removing volumes.", Fore.RED, logging.ERROR)

        printc("All services have been stopped.", Fore.GREEN)
        return True
    except Exception as e:
        printc(f"Error stopping services: {e}", Fore.RED, logging.ERROR)
        return False


def print_troubleshooting_tips() -> None:
    """Print common troubleshooting tips."""
    printc("\n===== Troubleshooting Guide =====", Fore.CYAN)
    printc("Common issues and solutions:", Fore.CYAN)

    printc("\n1. Services won't start:", Fore.YELLOW)
    printc("   - Verify Docker is running: docker info", Fore.WHITE)
    printc("   - Check logs: docker logs redis-mqtt-sync", Fore.WHITE)
    printc("   - Verify network connectivity to MQTT broker", Fore.WHITE)

    printc("\n2. Configuration problems:", Fore.YELLOW)
    printc("   - Check env.conf for correct values", Fore.WHITE)
    printc("   - Verify REDIS_HOST and MQTT_HOST are reachable", Fore.WHITE)
    printc("   - Test credentials for MQTT and Redis", Fore.WHITE)

    printc("\n3. Redis connection issues:", Fore.YELLOW)
    printc("   - If using local Redis: docker logs redis-cache-mqtt", Fore.WHITE)
    printc("   - If external: verify firewall/network allows connection", Fore.WHITE)
    printc(
        "   - Test connection: redis-cli -h $REDIS_HOST -p $REDIS_PORT ping", Fore.WHITE
    )

    printc("\n4. MQTT connection issues:", Fore.YELLOW)
    printc("   - Verify broker is accepting connections", Fore.WHITE)
    printc("   - Check credentials in env.conf", Fore.WHITE)
    printc("   - If using TLS, verify certificates are valid", Fore.WHITE)

    printc("\nFor additional help, check logs at:", Fore.CYAN)
    printc(f"- Script logs: {LOG_FILE}", Fore.WHITE)
    printc("- Docker logs: docker logs <container-name>", Fore.WHITE)
    printc("===============================\n", Fore.CYAN)


def main() -> int:
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description="Manage Redis-MQTT Sync Services")
    subparsers = parser.add_subparsers(dest="action", help="Action to perform")

    # Start command
    start_parser = subparsers.add_parser(
        "start", help="Build and start services with Docker Compose."
    )
    start_parser.add_argument(
        "--services",
        nargs="+",
        help="Specify which services to start (e.g., redis-mqtt-sync redis). Defaults to: redis-mqtt-sync, redis.",
        choices=[
            "redis-mqtt-sync",
            "redis",
        ],  # List available services (no mqtt-broker)
        metavar="SERVICE",
    )

    # Stop command
    stop_parser = subparsers.add_parser(
        "stop", help="Stop and remove services defined in Docker Compose."
    )

    # Add troubleshoot command
    troubleshoot_parser = subparsers.add_parser(
        "troubleshoot", help="Show troubleshooting information."
    )

    args = parser.parse_args()

    if not args.action:
        parser.print_help()
        return 0

    if args.action == "troubleshoot":
        print_troubleshooting_tips()
        return 0

    if not DOCKER_DIR.exists() or not COMPOSE_FILE.exists():
        printc(
            f"Error: Cannot find Docker configuration at {DOCKER_DIR}",
            Fore.RED,
            logging.ERROR,
        )
        printc("\nTroubleshooting steps:", Fore.YELLOW)
        printc("1. Check that you're in the correct directory", Fore.YELLOW)
        printc("2. Verify the project structure is intact", Fore.YELLOW)
        printc(f"3. Expected path: {COMPOSE_FILE}", Fore.YELLOW)
        printc("4. Try re-cloning the repository if files are missing", Fore.YELLOW)
        return 1

    try:
        if args.action == "start":
            # Services are already validated through argparse choices
            success = start_services(args.services)
        elif args.action == "stop":
            success = stop_services()
        else:
            parser.print_help()
            return 0

        return 0 if success else 1
    except ConfigurationError as e:
        printc(f"Configuration error: {e}", Fore.RED, logging.ERROR)
        printc(
            "Run 'python manage_redis_with_sync_to_mqtt.py troubleshoot' for help",
            Fore.YELLOW,
        )
        return 1
    except DockerError as e:
        printc(f"Docker error: {e}", Fore.RED, logging.ERROR)
        printc(
            "Run 'python manage_redis_with_sync_to_mqtt.py troubleshoot' for help",
            Fore.YELLOW,
        )
        return 1
    except Exception as e:
        printc(f"Unhandled error: {e}", Fore.RED, logging.ERROR)
        printc(
            "Run 'python manage_redis_with_sync_to_mqtt.py troubleshoot' for help",
            Fore.YELLOW,
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
