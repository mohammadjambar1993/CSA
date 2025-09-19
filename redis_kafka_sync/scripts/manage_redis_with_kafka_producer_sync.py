import argparse
import logging
import os
import re
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

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
LOG_FILE = SCRIPT_DIR / "redis_with_kafka_producer_sync.log"

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


def validate_stack_name(stack_name: str) -> bool:
    """
    Validate that a stack name contains only alphanumeric characters,
    dashes, and underscores to prevent command injection.
    """
    if not stack_name or not isinstance(stack_name, str):
        return False
    # Only allow alphanumeric chars, underscores, and dashes
    return bool(re.match(r"^[a-zA-Z0-9_-]+$", stack_name))


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


def check_swarm():
    """Check if Docker is in swarm mode, initialize if not."""
    returncode, stdout, stderr = run_command(["docker", "node", "ls"], check=False)
    if returncode != 0:
        printc(
            "Docker is not in swarm mode. Initializing swarm...",
            Fore.YELLOW,
            logging.WARNING,
        )
        try:
            returncode, stdout, stderr = run_command(["docker", "swarm", "init"])
            if returncode != 0:
                printc(
                    f"Error initializing Docker Swarm: {stderr}",
                    Fore.RED,
                    logging.ERROR,
                )
                return False
            return True
        except Exception as e:
            printc(f"Error initializing Docker Swarm: {e}", Fore.RED, logging.ERROR)
            return False
    return True


def ensure_network():
    """Ensure the EDN network exists."""
    try:
        returncode, stdout, stderr = run_command(["docker", "network", "ls"])
        if returncode != 0:
            raise DockerError(f"Failed to list Docker networks: {stderr}")

        if "edn-network" not in stdout:
            printc(
                "Network 'edn-network' not found. Creating it...",
                Fore.YELLOW,
                logging.WARNING,
            )
            returncode, stdout, stderr = run_command(
                [
                    "docker",
                    "network",
                    "create",
                    "--driver",
                    "overlay",
                    "--attachable",
                    "edn-network",
                ]
            )
            if returncode != 0:
                raise DockerError(f"Failed to create edn-network: {stderr}")
            printc("Network 'edn-network' created successfully.", Fore.GREEN)
        return True
    except Exception as e:
        printc(f"Error ensuring network: {e}", Fore.RED, logging.ERROR)
        return False


def swarm_deploy(stack_name):
    """Deploy reactive producer to Docker Swarm."""
    printc("Deploying Reactive Producer to Docker Swarm", Fore.GREEN)

    # Validate stack name to prevent command injection
    if not validate_stack_name(stack_name):
        printc(
            f"Error: Invalid stack name '{stack_name}'. Only alphanumeric characters, dashes, and underscores are allowed.",
            Fore.RED,
            logging.ERROR,
        )
        return False

    # Validate prerequisites
    if not DOCKER_DIR.exists():
        printc("Error: Cannot find docker directory.", Fore.RED, logging.ERROR)
        return False

    if not check_docker() or not check_swarm():
        return False

    if not ENV_CONF.exists():
        printc(
            "Warning: env.conf file not found. Using default configuration.",
            Fore.YELLOW,
            logging.WARNING,
        )
        printc(
            "Copy env.conf.example to env.conf and modify it for your environment.",
            Fore.YELLOW,
            logging.WARNING,
        )
    else:
        printc("Loading environment variables from env.conf...", Fore.GREEN)

    # Ensure network exists
    if not ensure_network():
        return False

    try:
        env = load_env_conf()
        compose_file = DOCKER_DIR / "docker-compose.swarm.yml"

        if not compose_file.exists():
            raise ConfigurationError(f"Compose file not found: {compose_file}")

        printc(f"Deploying stack '{stack_name}' to swarm...", Fore.GREEN)
        returncode, stdout, stderr = run_command(
            ["docker", "stack", "deploy", "-c", str(compose_file), stack_name],
            cwd=DOCKER_DIR,
            env=env,
        )

        if returncode != 0:
            raise DockerError(f"Failed to deploy stack: {stderr}")

        printc(f"Stack '{stack_name}' deployed successfully!", Fore.GREEN)
        printc("Reactive Producer is running in swarm mode.", Fore.GREEN)
        printc("To view services: docker service ls", Fore.YELLOW)
        printc(
            f"To view logs: docker service logs {stack_name}_redis-producer",
            Fore.YELLOW,
        )
        printc(f"To remove stack: docker stack rm {stack_name}", Fore.YELLOW)

        printc("\nService Status:", Fore.GREEN)
        returncode, status_output, stderr = run_command(
            ["docker", "service", "ls", "--filter", f"name={stack_name}"],
            cwd=DOCKER_DIR,
            env=env,
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

        # Print Redis/Kafka config
        redis_streams = None
        kafka_topic = None
        replicas = None

        if ENV_CONF.exists():
            try:
                with open(ENV_CONF) as f:
                    for line in f:
                        if line.startswith("REDIS_STREAMS="):
                            redis_streams = line.strip().split("=", 1)[1]
                        elif line.startswith("KAFKA_TOPIC="):
                            kafka_topic = line.strip().split("=", 1)[1]
                        elif line.startswith("REPLICAS="):
                            replicas = line.strip().split("=", 1)[1]
            except Exception as e:
                printc(
                    f"Warning: Could not parse environment config: {e}",
                    Fore.YELLOW,
                    logging.WARNING,
                )

        printc(f"\nRedis Streams: {redis_streams or 'biosignal-stream'}", Fore.YELLOW)
        printc(f"Kafka Topics: {kafka_topic or 'biosignal-topic'}", Fore.YELLOW)
        printc(f"Replicas: {replicas or '1'}", Fore.YELLOW)
        return True
    except Exception as e:
        printc(f"Error deploying stack: {e}", Fore.RED, logging.ERROR)
        return False


def run_compose_command(
    compose_file: Path, command_args: List[str], services: List[str] = None
):
    """Runs a docker-compose command using a specific compose file, optionally targeting services."""
    if not check_docker() or not check_docker_compose():
        return None

    if not compose_file.exists():
        printc(
            f"Error: Compose file not found: {compose_file}", Fore.RED, logging.ERROR
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

    base_command = ["docker-compose", "-f", str(compose_file)]
    action = command_args[0]
    options = command_args[1:]

    full_command = base_command + [action] + options
    if services and action == "up":  # Only add services for 'up' command
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

        return stdout
    except Exception as e:
        printc(f"Error executing docker-compose command: {e}", Fore.RED, logging.ERROR)
        return None


def start_services(services_to_start: Optional[List[str]] = None):
    """Start Redis-Kafka Producer Services in non-swarm mode."""
    printc("Starting Redis-Kafka Producer Services (Non-Swarm Mode)", Fore.GREEN)

    if not ENV_CONF.exists():
        printc(
            "Warning: env.conf file not found. Using default configuration.",
            Fore.YELLOW,
            logging.WARNING,
        )

    # Services in docker-compose.no_swarm.yml are typically 'reactive-producer' and 'redis'
    default_services = ["reactive-producer", "redis"]
    compose_file = DOCKER_DIR / "docker-compose.no_swarm.yml"

    if not compose_file.exists():
        printc(
            f"Error: Compose file not found at {compose_file}", Fore.RED, logging.ERROR
        )
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

        if services_to_start is None:
            services_to_start = default_services
            printc(
                f"Starting default services: {', '.join(services_to_start)}...",
                Fore.GREEN,
            )
        else:
            # Ensure the main service is always included
            if "reactive-producer" not in services_to_start:
                printc(
                    "Warning: 'reactive-producer' service not specified, adding it.",
                    Fore.YELLOW,
                    logging.WARNING,
                )
                services_to_start.append("reactive-producer")
            printc(
                f"Starting specified services: {', '.join(services_to_start)}...",
                Fore.GREEN,
            )

        printc("Building and starting services...", Fore.GREEN)
        result = run_compose_command(
            compose_file, ["up", "--build", "-d"], services=services_to_start
        )

        if result is None:
            printc("Failed to start services.", Fore.RED, logging.ERROR)
            return False

        printc("Services started successfully!", Fore.GREEN)
        printc(
            f"Selected services are running in the background (using {compose_file.name}).",
            Fore.GREEN,
        )
        printc(
            f"To view logs: docker-compose -f {compose_file} logs -f {' '.join(services_to_start)}",
            Fore.YELLOW,
        )
        printc(f"To stop services: python {Path(__file__).name} stop", Fore.YELLOW)
        return True
    except Exception as e:
        printc(f"Error starting services: {e}", Fore.RED, logging.ERROR)
        return False


def stop_services():
    """Stop Redis-Kafka Producer Services in non-swarm mode."""
    printc("Stopping Redis-Kafka Producer Services (Non-Swarm Mode)...", Fore.YELLOW)
    compose_file = DOCKER_DIR / "docker-compose.no_swarm.yml"

    if not compose_file.exists():
        printc(
            f"Error: Compose file not found at {compose_file}. Cannot guarantee proper shutdown.",
            Fore.RED,
            logging.ERROR,
        )
        return False

    try:
        result = run_compose_command(compose_file, ["down"])
        if result is None:
            printc("Error stopping services.", Fore.RED, logging.ERROR)
            return False

        printc("Non-swarm services stopped successfully.", Fore.GREEN)

        try:
            response = input(
                "Do you want to remove all related Docker volumes? (y/N): "
            )
        except EOFError:
            response = "n"

        if response.strip().lower() in ["y", "yes"]:
            printc("Removing Docker volumes...", Fore.YELLOW)
            try:
                # Only remove volumes matching reactive_producer
                returncode, stdout, stderr = run_command(
                    ["docker", "volume", "ls", "-q"]
                )
                if returncode == 0:
                    volumes = stdout.splitlines()
                    removed_any = False
                    for v in volumes:
                        # Strict validation of volume name before removal
                        if "reactive_producer" in v and re.match(
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
    parser = argparse.ArgumentParser(
        description="Manage Redis-Kafka Producer Service (Both Docker Compose and Swarm)"
    )
    subparsers = parser.add_subparsers(dest="command", help="Action to perform")

    # --- Docker Compose (Non-Swarm) Commands ---
    start_parser = subparsers.add_parser(
        "start",
        help="Build and start services locally using Docker Compose (non-swarm).",
    )
    start_parser.add_argument(
        "--services",
        nargs="+",
        help="Specify which non-swarm services to start (e.g., reactive-producer). Defaults to all: reactive-producer, redis.",
        choices=[
            "reactive-producer",
            "redis",
        ],  # Services from docker-compose.no_swarm.yml
        metavar="SERVICE",
    )

    stop_parser = subparsers.add_parser(
        "stop", help="Stop and remove local services started with 'start'."
    )
    logging.info(f"Stopping services..., result: {stop_parser}")

    # --- Docker Swarm Commands ---
    swarm_parser = subparsers.add_parser(
        "swarm", help="Manage the service deployment on Docker Swarm."
    )
    swarm_parser.add_argument(
        "--stack-name",
        default="producer-stack",
        help="Name of the Docker Swarm stack (default: producer-stack)",
    )
    swarm_parser.add_argument(
        "--action",
        choices=["deploy", "remove"],
        default="deploy",
        help="Action to perform: deploy or remove the stack.",
    )
    swarm_parser.add_argument(
        "--replicas",
        type=int,
        help="Number of replicas for the reactive-producer service (deploy only).",
    )

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 0

    try:
        if args.command == "start":
            # Services are already validated through argparse choices
            success = start_services(args.services)
        elif args.command == "stop":
            success = stop_services()
        elif args.command == "swarm":
            # Ensure docker dir exists before proceeding with swarm actions
            if not DOCKER_DIR.exists():
                printc(
                    f"Error: Docker directory not found at {DOCKER_DIR}",
                    Fore.RED,
                    logging.ERROR,
                )
                return 1

            try:
                # Validate the stack name
                if not validate_stack_name(args.stack_name):
                    printc(
                        f"Error: Invalid stack name '{args.stack_name}'. Only alphanumeric characters, dashes, and underscores are allowed.",
                        Fore.RED,
                        logging.ERROR,
                    )
                    return 1

                env = load_env_conf()
                # Set number of replicas in environment if specified
                if args.replicas is not None and args.action == "deploy":
                    if args.replicas <= 0:
                        printc(
                            "Warning: Invalid replicas value. Using default (1).",
                            Fore.YELLOW,
                            logging.WARNING,
                        )
                        os.environ["REPLICAS"] = "1"
                    else:
                        os.environ["REPLICAS"] = str(args.replicas)
                        printc(f"Setting REPLICAS={args.replicas}", Fore.GREEN)

                if not ENV_CONF.exists():
                    printc(
                        "Warning: env.conf file not found. Using default configuration.",
                        Fore.YELLOW,
                        logging.WARNING,
                    )
                    printc(
                        "Copy env.conf.example to env.conf and modify it for your environment.",
                        Fore.YELLOW,
                        logging.WARNING,
                    )
                else:
                    printc("Loading environment variables from env.conf...", Fore.GREEN)

                ensure_network()

                if args.action == "deploy":
                    success = swarm_deploy(args.stack_name)
                elif args.action == "remove":
                    printc(
                        f"Removing stack '{args.stack_name}' from Docker Swarm...",
                        Fore.YELLOW,
                    )
                    returncode, stdout, stderr = run_command(
                        ["docker", "stack", "rm", args.stack_name],
                        cwd=DOCKER_DIR,
                        env=env,
                    )
                    if returncode == 0:
                        printc(
                            f"Stack '{args.stack_name}' removed successfully!",
                            Fore.GREEN,
                        )
                        success = True
                    else:
                        printc(
                            f"Error removing stack: {stderr}", Fore.RED, logging.ERROR
                        )
                        success = False
                else:
                    # This shouldn't happen due to argparse choices, but just in case
                    printc(
                        "Invalid action specified. Use 'deploy' or 'remove'.",
                        Fore.RED,
                        logging.ERROR,
                    )
                    success = False
            except Exception as e:
                printc(f"Error in swarm operation: {e}", Fore.RED, logging.ERROR)
                success = False
        else:
            parser.print_help()
            return 0

        return 0 if success else 1
    except Exception as e:
        printc(f"Unhandled error: {e}", Fore.RED, logging.ERROR)
        return 1


if __name__ == "__main__":
    sys.exit(main())
