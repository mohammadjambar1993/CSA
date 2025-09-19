"""
This file is added for stability and back-compatibility with older setuptools versions. In the future all the logic
can be moved to the pyproject.toml file
"""

import os

from setuptools import setup

setup()


# def parse_requirements(filename):
#     """Parse requirements from a file."""
#     with open(filename, 'r') as f:
#         lines = f.readlines()
#     # Remove comments and empty lines
#     requirements = [line.strip() for line in lines if line.strip() and not line.startswith('#')]
#     return requirements
#
#
# # Ensure dev-requirements.txt exists
# requirements_path = os.path.join(os.path.dirname(__file__), 'dev-requirements.txt')
# if os.path.exists(requirements_path):
#     install_requires = parse_requirements(requirements_path)
# else:
#     install_requires = []
#
# setup(
#     install_requires=install_requires,
# )
