from setuptools import setup, find_packages

setup(
    name="meowdb",
    version="0.1.0",
    description="Python client for MeowDB - Ultra-fast Key-Value & Secondary Index Database",
    author="MeowDB Team",
    packages=find_packages(),
    python_requires=">=3.8",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
)
