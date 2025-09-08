from setuptools import setup, find_packages

setup(
    name="cooling-watchdog",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pandas",
        "requests",
        "openpyxl"
    ],
    entry_points={
        'console_scripts': [
            'cooling-watchdog=cooling_watchdog.main:main',
        ],
    },
)