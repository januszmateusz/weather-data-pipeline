"""
Setup configuration for Weather Data Pipeline
"""

from setuptools import setup, find_packages
from pathlib import Path

# Read the contents of README file
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text(encoding="utf-8")

# Read requirements
requirements = (this_directory / "requirements.txt").read_text().splitlines()
requirements = [req.strip() for req in requirements if req.strip() and not req.startswith("#")]

setup(
    name="weather-data-pipeline",
    version="1.0.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="A robust ETL pipeline for fetching and processing weather data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/weather-data-pipeline",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    extras_require={
        "dev": [
            "pytest>=7.4.3",
            "pytest-cov>=4.1.0",
            "pytest-mock>=3.12.0",
            "black>=23.12.1",
            "flake8>=7.0.0",
            "mypy>=1.8.0",
        ],
        "viz": [
            "matplotlib>=3.8.2",
            "seaborn>=0.13.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "weather-pipeline=main:run_pipeline",
        ],
    },
    keywords="weather api etl data-pipeline openweathermap",
    project_urls={
        "Bug Reports": "https://github.com/yourusername/weather-data-pipeline/issues",
        "Source": "https://github.com/yourusername/weather-data-pipeline",
        "Documentation": "https://github.com/yourusername/weather-data-pipeline#readme",
    },
)