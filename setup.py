from setuptools import setup
from setuptools_rust import Binding, RustExtension

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="cowabunga_rs",
    version="0.1.0",
    author="Group",
    description="Rust database package",
    author_email="",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="",
    rust_extensions=[
        RustExtension("cowabunga_rs", binding=Binding.PyO3),
    ],
    packages=["cowabunga_rs"],
    zip_safe=False,
)
