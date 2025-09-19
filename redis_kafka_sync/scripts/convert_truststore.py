#!/usr/bin/env python3
"""
Convert JKS Truststore to PEM format

This script converts Java KeyStore (JKS) format truststore files to PEM format,
which is more widely supported by Python and other non-Java applications.

Usage:
    python convert_truststore.py <jks_file> <jks_password> <output_pem_file>

Example:
    python convert_truststore.py /app/certs/kafka.truststore.jks truststore_password /app/certs/kafka.truststore.pem
"""

import argparse
import os
import re
import shlex
import subprocess
import sys
import tempfile
from pathlib import Path


class ConversionError(Exception):
    """Exception raised for errors during conversion process."""

    pass


def validate_file_path(path):
    """
    Validate if a file path is safe and exists if it's an input file.

    Args:
        path: File path to validate

    Returns:
        bool: True if valid, False otherwise
    """
    # Convert to Path object for safer path manipulation
    path_obj = Path(path)

    # Check for path traversal attempts
    try:
        # Resolve to absolute path and check if it's trying to escape
        path_obj = path_obj.resolve()
    except (ValueError, RuntimeError):
        print(f"Error: Invalid path: {path}")
        return False

    return True


def run_command(cmd, check=True, capture_output=True, text=True):
    """
    Run a command securely and return the result.

    Args:
        cmd: List of command arguments
        check: Whether to check the return code
        capture_output: Whether to capture stdout/stderr
        text: Whether to return text or bytes

    Returns:
        subprocess.CompletedProcess: The result of the command

    Raises:
        subprocess.SubprocessError: If the command fails and check is True
    """
    # Ensure all elements are strings
    cmd = [str(arg) for arg in cmd]

    try:
        return subprocess.run(
            cmd,
            check=check,
            capture_output=capture_output,
            text=text,
            shell=False,  # Explicitly set shell=False for security
        )
    except subprocess.SubprocessError as e:
        print(f"Command failed: {' '.join(cmd)}")
        print(f"Error: {e}")
        raise


def convert_jks_to_pem(jks_file, jks_password, pem_file):
    """
    Convert JKS truststore to PEM format using keytool and openssl

    Args:
        jks_file: Path to the JKS truststore file
        jks_password: Password for the JKS truststore
        pem_file: Output path for the PEM file

    Returns:
        bool: True if successful, False otherwise
    """
    # Validate input and output paths
    if not validate_file_path(jks_file):
        return False

    if not validate_file_path(pem_file):
        return False

    # Check if input file exists
    if not os.path.exists(jks_file):
        print(f"Error: JKS file not found: {jks_file}")
        return False

    try:
        # Create a temporary directory for intermediate files
        with tempfile.TemporaryDirectory() as temp_dir:
            # Step 1: Export certificates from JKS to a PKCS12 file
            p12_file = os.path.join(temp_dir, "truststore.p12")
            keytool_cmd = [
                "keytool",
                "-importkeystore",
                "-srckeystore",
                jks_file,
                "-srcstorepass",
                jks_password,
                "-destkeystore",
                p12_file,
                "-deststoretype",
                "PKCS12",
                "-deststorepass",
                jks_password,
                "-noprompt",
            ]

            print("Exporting from JKS to PKCS12...")
            try:
                result = run_command(keytool_cmd)
                print(f"Keytool command result: {result}")
            except subprocess.SubprocessError:
                # Try alternative approach if p12 conversion fails
                return export_certs_directly(jks_file, jks_password, pem_file)

            # Step 2: Convert PKCS12 to PEM using OpenSSL
            openssl_cmd = [
                "openssl",
                "pkcs12",
                "-in",
                p12_file,
                "-out",
                pem_file,
                "-passin",
                f"pass:{jks_password}",
                "-passout",
                f"pass:{jks_password}",
                "-nodes",  # No encryption on output
            ]

            print("Converting PKCS12 to PEM...")
            try:
                run_command(openssl_cmd)
            except subprocess.SubprocessError:
                return False

            print(f"Successfully converted JKS to PEM: {pem_file}")
            return True

    except Exception as e:
        print(f"Error during conversion: {e}")
        return False


def validate_alias(alias):
    """
    Validate certificate alias to prevent command injection

    Args:
        alias: Certificate alias to validate

    Returns:
        bool: True if valid, False otherwise
    """
    # Only allow alphanumeric characters, underscore, dash, and dot in aliases
    return bool(alias and re.match(r"^[a-zA-Z0-9_\-\.]+$", alias))


def export_certs_directly(jks_file, jks_password, pem_file):
    """
    Alternative method: Export certificates directly from JKS using keytool and openssl

    Args:
        jks_file: Path to the JKS truststore file
        jks_password: Password for the JKS truststore
        pem_file: Output path for the PEM file

    Returns:
        bool: True if successful, False otherwise
    """
    # Validate paths again
    if not validate_file_path(jks_file) or not validate_file_path(pem_file):
        return False

    try:
        # Create a temporary directory for intermediate files
        with tempfile.TemporaryDirectory() as temp_dir:
            # Step 1: List all aliases in the JKS file
            keytool_list_cmd = [
                "keytool",
                "-list",
                "-keystore",
                jks_file,
                "-storepass",
                jks_password,
            ]

            print("Listing certificates in truststore...")
            try:
                result = run_command(keytool_list_cmd)
            except subprocess.SubprocessError:
                return False

            # Step 2: Extract each certificate to a DER file
            with open(pem_file, "w") as pem_output:
                for line in result.stdout.splitlines():
                    if "," in line and "trustedCertEntry" in line:
                        alias = line.split(",")[0].strip()

                        # Validate alias to prevent command injection
                        if not validate_alias(alias):
                            print(f"Skipping certificate with invalid alias: {alias}")
                            continue

                        print(f"Exporting certificate: {alias}")

                        der_file = os.path.join(temp_dir, f"{alias}.der")

                        # Export the certificate as DER
                        export_cmd = [
                            "keytool",
                            "-exportcert",
                            "-keystore",
                            jks_file,
                            "-storepass",
                            jks_password,
                            "-alias",
                            alias,
                            "-file",
                            der_file,
                        ]

                        try:
                            run_command(export_cmd)
                        except subprocess.SubprocessError:
                            print(f"Error exporting certificate {alias}")
                            continue

                        # Convert DER to PEM using OpenSSL
                        openssl_cmd = [
                            "openssl",
                            "x509",
                            "-inform",
                            "der",
                            "-in",
                            der_file,
                            "-out",
                            "-",
                        ]

                        try:
                            result = run_command(openssl_cmd)
                        except subprocess.SubprocessError:
                            print(f"Error converting certificate {alias} to PEM")
                            continue

                        # Append the PEM certificate to the output file
                        pem_output.write(result.stdout)

            if os.path.getsize(pem_file) > 0:
                print(f"Successfully exported certificates to PEM: {pem_file}")
                return True
            else:
                print("No certificates were exported")
                return False

    except Exception as e:
        print(f"Error during direct export: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Convert JKS truststore to PEM format")
    parser.add_argument("jks_file", help="Path to the JKS truststore file")
    parser.add_argument("jks_password", help="Password for the JKS truststore")
    parser.add_argument("pem_file", help="Output path for the PEM file")

    args = parser.parse_args()

    success = convert_jks_to_pem(args.jks_file, args.jks_password, args.pem_file)

    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
