import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pulse-broker",
    version="0.1.0",
    author="Marcos Rosa",
    author_email="marcos@example.com",
    description="Python SDK for Pulse Broker",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/marcosrosa/pulse",
    project_urls={
        "Bug Tracker": "https://github.com/marcosrosa/pulse/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "."},
    packages=setuptools.find_packages(where="."),
    python_requires=">=3.7",
    install_requires=[
        "grpcio>=1.50.0",
        "protobuf>=4.21.0",
        "pyyaml>=6.0",
    ],
)
