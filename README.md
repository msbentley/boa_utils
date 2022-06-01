# boa_utils
A python package for working with the BepiColombo Operational Archive (BOA). BOA offers a TAP interface for querying the archive contents using ADQL, and a separate download endpoint which allows users to retrieve packaged data.

BOA access is restricted to authorised users, and access credentials are specified in a YAML configuration file.

## Dependencies

The following dependencies must be met:
- python >=3.6
- matplotlib
- numpy
- astropy
- pandas
- pyyaml
- requests

## Installation

### conda

First, clone this repository. If you are using conda, the dependencies can be installed in a new environment using the provided environment file:

```conda env create -f environment.yml```

The newly created environment can be activated with:

```conda activate boa_utils```

Otherwise, please make sure the dependencies are installed with your system package manager, or a tool like `pip`. Use of a conda environment or virtualenv is recommended!

The package can then be installed with:

```python setup.py install```


## URL

The URL for the BOA can be specified when instantiating the BOA class. If none is given, a default URL is used, which corresponds to the default operational server.

## Authentication

Access to BOA needs authentication. This is controlled by a config file which can be pointed to by the `config_file` parameter when instantiating the BOA class, for example:

```python
boa = boa_utils.BOA(config_file='/path/to/a/config_file.yml')
```
The configuration file should be in YAML format and contain the username and password as follows:

```yaml
user:
    login: "userone"
    password: "blah"
```

## Example

The Jupyter notebook included with this repository shows an example of how to use the code.  To view the notebook, click [here](https://nbviewer.jupyter.org/github/msbentley/boa_utils/blob/main/boa_utils.ipynb).

