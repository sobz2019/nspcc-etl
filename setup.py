from setuptools import setup, find_packages
import os

# Read requirements from requirements.txt
with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name="nspcc_etl",
    version="0.1",
    packages=find_packages(),
    install_requires=requirements,
    python_requires='>=3.6',
    description="NSPCC ETL Process",
    author="NSPCC Team",
)