from setuptools import setup
from setuptools_rust import Binding, RustExtension

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="ecs_165_database",
    version="0.1.0",
    author="Group",
    description="Rust database package",
    author_email="",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="",
    rust_extensions=[
        RustExtension("ecs_165_database", binding=Binding.PyO3),
    ],
    packages=["ecs_165_database"],
    zip_safe=False,
)
