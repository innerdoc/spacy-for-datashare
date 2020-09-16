import pathlib
from setuptools import setup, find_packages

setup(
    name="spacy-for-datashare",
    version="0.1.0",
    author="Rob van Zoest",
    packages=find_packages(exclude=["notebooks", "docs"]),
    description="Let spaCy do the parsing of Named Entities for documents in the Datashare platform.",
    long_description=pathlib.Path("README.md").read_text(),
    long_description_content_type="text/markdown",
    url="https://github.com/innerdoc/spacy-for-datashare",
    project_urls={
        "Documentation": "https://github.com/innerdoc/spacy-for-datashare",
        "Source Code": "https://github.com/innerdoc/spacy-for-datashare",
        "Issue Tracker": "https://github.com/innerdoc/spacy-for-datashare/issues",
    },
    install_requires=[
    "tqdm>=4.0.0",
    "spacy>=2.2.0",
    "price_parser>=0.3.0",
    ],
    classifiers=[
        "Intended Audience :: Science/Research",
        "Intended Audience :: Developers",
        "Intended Audience :: Education",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "License :: OSI Approved :: MIT License",
        "Topic :: Scientific/Engineering",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "Topic :: Scientific/Engineering :: Information Analysis",
        "Topic :: Text Processing",
    ],
)