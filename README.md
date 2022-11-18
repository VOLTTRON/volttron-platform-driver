# Platform Driver Agent

![Passing?](https://github.com/eclipse-volttron/volttron-platform-driver/actions/workflows/run-tests.yml/badge.svg)
[![pypi version](https://img.shields.io/pypi/v/volttron-platform-driver.svg)](https://pypi.org/project/volttron-platform-driver/)


The Platform Driver agent is a special purpose agent a user can install on the platform to manage communication of the platform with devices. The Platform driver features a number of endpoints for collecting data and sending control signals using the message bus and automatically publishes data to the bus on a specified interval.

# Prerequisites

* Python 3.8

## Python

<details>
<summary>To install Python 3.8, we recommend using <a href="https://github.com/pyenv/pyenv"><code>pyenv</code></a>.</summary>

```bash
# install pyenv
git clone https://github.com/pyenv/pyenv ~/.pyenv

# setup pyenv (you should also put these three lines in .bashrc or similar)
export PATH="${HOME}/.pyenv/bin:${PATH}"
export PYENV_ROOT="${HOME}/.pyenv"
eval "$(pyenv init -)"

# install Python 3.8
pyenv install 3.8.10

# make it available globally
pyenv global system 3.8.10
```
</details>

# Installation

Create and activate a virtual environment.

```shell
python -m venv env
source env/bin/activate
```

Installing volttron-platform-driver requires a running volttron instance.

```shell
pip install volttron

# Start platform with output going to volttron.log
volttron -vv -l volttron.log &
```

Install and start the volttron-platform-driver.

```shell
vctl install volttron-platform-driver --vip-identity platform.driver --start
```

View the status of the installed agent

```shell
vctl status
```

# Development

See [Developing on Modular Volttron](https://github.com/eclipse-volttron/volttron-core/blob/develop/DEVELOPING_ON_MODULAR.md).


# Git Setup

1. To use git to manage version control, create a new git repository in your local agent project.

```
git init
```

2. Then create a new repo in your Github or Gitlab account. Copy the URL that points to that new repo in
your Github or Gitlab account. This will be known as our 'remote'.

3. Add the remote (i.e. the new repo URL from your Github or Gitlab account) to your local repository. Run the following command:

```git remote add origin <my github/gitlab URL>```

When you push to your repo, note that the default branch is called 'main'.


# Documentation

To build the docs, navigate to the 'docs' directory and build the documentation:

```shell
cd docs
make html
```

After the documentation is built, view the documentation in html form in your browser.
The html files will be located in `~<path to agent project directory>/docs/build/html`.

ℹ️ **PROTIP: To open the landing page of your documentation directly from the command line, run the following command:**

```shell
open <path to agent project directory>/docs/build/html/index.html
```

This will open the documentation landing page in your default browsert (e.g. Chrome, Firefox).

# Disclaimer Notice

This material was prepared as an account of work sponsored by an agency of the
United States Government.  Neither the United States Government nor the United
States Department of Energy, nor Battelle, nor any of their employees, nor any
jurisdiction or organization that has cooperated in the development of these
materials, makes any warranty, express or implied, or assumes any legal
liability or responsibility for the accuracy, completeness, or usefulness or any
information, apparatus, product, software, or process disclosed, or represents
that its use would not infringe privately owned rights.

Reference herein to any specific commercial product, process, or service by
trade name, trademark, manufacturer, or otherwise does not necessarily
constitute or imply its endorsement, recommendation, or favoring by the United
States Government or any agency thereof, or Battelle Memorial Institute. The
views and opinions of authors expressed herein do not necessarily state or
reflect those of the United States Government or any agency thereof.
