from setuptools import find_packages, setup

setup(
    name="abn_extract",
    packages=find_packages(exclude=["abn_extract_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
