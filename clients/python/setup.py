from setuptools import setup, find_packages

setup(
    name="veloxdb",
    version="0.1.0",
    description="Official Python client for VeloxDB - Ultra-fast Key-Value & Secondary Index Database",
    author="VeloxDB Team",
    packages=find_packages(),
    python_requires=">=3.8",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
        "License :: OSI Approved :: MIT License",
    ],
)
