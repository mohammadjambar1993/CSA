from setuptools import find_packages, setup

setup(
    name="edn_service_bus",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "aiokafka",
        "redis",
        "pytest",
        "pytest-asyncio",
        "pytest-cov",
    ],
    python_requires=">=3.10",
)
