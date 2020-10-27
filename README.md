# DEAfrica Airflow
A repository containing Airflow DAGs that are used for Digital Earth Africa.

## Development

### Local Editing of DAG's

DAG's can be locally edited and validated. Development can be done in `conda` or `venv` according to developer preference. Grab everything airflow and write DAG's. Use `autopep8` and `pylint` to achieve import validation and consistent formatting as the CI pipeline for this repository matures.

```bash
pip install apache-airflow[aws,kubernetes,postgres,redis,ssh,celery]
pip install pylint pylint-airflow

pylint dags plugins
```


## Pre-commit setup

Install pip modules

```bash
    pip install apache-airflow[aws,kubernetes,postgres,redis,ssh,celery]==1.10.11
    pip install shapely pyproj
    pip install pylint pylint-airflow
```

A [pre-commit](https://pre-commit.com/) config is provided to automatically format
and check your code changes. This allows you to immediately catch and fix
issues before you raise a failing pull request (which run the same checks under
Travis).

If you don't use Conda, install pre-commit from pip:

    pip install pre-commit

If you do use Conda, install from conda-forge (*required* because the pip
version uses virtualenvs which are incompatible with Conda's environments)

    conda install pre_commit

Now install the pre-commit hook to the current repository:

    pre-commit install

Your code will now be formatted and validated before each commit. You can also
invoke it manually by running `pre-commit run --all-files`
