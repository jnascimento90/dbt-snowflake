# dbt-snowflake
Testing dbt with the Snowflake 30-day free trial


## Local setup

To interact with Snowflake from the local machine, first run the following to install the dbt adapter

* Run `make init-venv` to install the dependencies.
* Activate the virtualenv by running `source ./venv/bin/activate`.

Setup your Snowflake credentials in `~/.dbt/profiles.yml`. The sample structure looks like this:

```
snowflake:
  outputs:
    dev:
      account: 
      database: 
      password: 
      role: 
      schema: 
      threads: 1
      type: snowflake
      user:
      warehouse: 
  target: dev
```

## Running dbt locally

First change the working directory with `cd snowflake` before running and dbt cli commands.

For example, you can create a table public.places by running

```
dbt seed --select places
```

## Integration with Airbyte

This repo was mainly used for testing the integration with Airbyte. For example, the model `people.py` runs after the "EL" part to apply the transformation on "family_name".
