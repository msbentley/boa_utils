from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(name='boa_utils',
    version='1.0-beta',
    author='Mark S. Bentley',
    author_email='mark@lunartech.org',
    description='Utilities to work with the BepiColombo Operational Archive',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/msbentley/boa_utils",
    download_url = 'https://github.com/msbentley/boa_utils/archive/v1.0-beta.tar.gz',
    install_requires=['pandas','pyyaml','astropy','numpy','matplotlib','requests'],
    python_requires='>=3.6',
    keywords = ['BOA','archive','BepiColombo'],
    zip_safe=False)
