# Requirements
Both app and dev dependencies from [pyproject.toml](../pyproject.toml) need to be installed 
in the environment. To install app/core dependencies, use
``` shell
pip install . 
```
To also install the (optional) dev dependencies, use
``` shell
pip install .[dev]  # some shells don't recognize .[dev] and need '.[dev]' 
```


# Linting and formatting
Local linting and formatting can be applied by subsequently (order is important) running
``` shell
isort src/  # imports alphabetically and automatically separates into sections and by type
```
then
``` shell
black src/  # formats code
```
and finally
``` shell
autoflake src  # removes unused imports and unused variables
```
from the terminal. 



# Code coverage
The `coverage` library can be used for measuring code coverage of our unit tests. Since we're using `pytest`, 
the coverage can be measured with 
```shell
coverage run -m pytest (path)
```
where `(path)` is an optional parameter, e.g. `tests/test_common.py`. Omitting `(path)` would run all discoverable 
unit tests. The results can subsequently displayed with
```shell
coverage report -m
```
