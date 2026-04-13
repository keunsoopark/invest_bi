Welcome to the dbt project!

[![CD](https://github.com/TelenorNorgeInternal/s07464-mirage-dbt/actions/workflows/cd.yml/badge.svg)](https://github.com/TelenorNorgeInternal/s07464-mirage-dbt/actions/workflows/cd.yml)

### Setting up (in Databricks)

If you're not familiar with using an integrated development environment (IDE), then you can use the Databricks UI when developing with dbt by following the steps below:
1. Log into your designated Databricks workspace using your apc account (`<eo/to><t-number>@apc.telenor.net`) with Azure Entra ID
- [Mirage prod workspace](https://adb-3754902905502248.8.azuredatabricks.net/)
- [Business prod workspace](https://adb-314398899072540.0.azuredatabricks.net/)
2. Link your Databricks account with your Github account by following the steps in [this guide](https://learn.microsoft.com/en-us/azure/databricks/repos/get-access-tokens-from-git-provider#--link-your-github-account-using-databricks-github-app).<br>
3. Clone this repository to Databricks by selecting `Workspace` > `Workspace` > `Repos` > `<eo/to><t-number>@apc.telenor.net` > `Create` > `Repo` > Add HTTPS repository link https://github.com/TelenorNorgeInternal/s07464-mirage-dbt.git <br>

<img src="./databricks_dbt/pics/add_repo_1.png" alt="add_repo_1" style="display: block; margin: 0 auto; width: 800px;" />
<img src="./databricks_dbt/pics/add_repo_2.png" alt="add_repo_2" style="display: block; margin: 0 auto; width: 400px;" />
<br>

4. Create a personal cluster by selecting `Compute` > `Create with dbt Development cluster policy for <user group>` from the dropdown menu. Change the environment variable `USER_INITIALS` to be your initials, e.g. `kpark`. <br>

<img src="./databricks_dbt/pics/create_personal_cluster_1.png" alt="create_personal_cluster_1" style="display: block; margin: 0 auto; width: 800px;" />
<img src="./databricks_dbt/pics/create_personal_cluster_2.png" alt="create_personal_cluster_2" style="display: block; margin: 0 auto; width: 400px;" />
<br>

<div style="border: 1px solid #cce5ff; background-color: #e8f4fd; padding: 10px; border-radius: 5px;">
  <strong>💡 Tip:</strong> Your cluster could be removed if you do not use it for a while. To keep your cluster, "pin" your cluster.
</div>

<img src="./databricks_dbt/pics/pin_cluster.png" alt="pin_cluster" style="display: block; margin: 0 auto; width: 400px;" />
<br>

5. In the repository that you cloned, open the notebook `run_dbt.py` located in the root, and attach your cluster. 
    - Make sure the `select` statement in the notebook corresponds to your project in the `models/` directory, and run the notebook. This notebook should now run the entire dbt project and produce its corresponding tables in your personal schema using `$USER_INITIALS`.
    - If you want to run a specific model, see [Running](#running) section or [dbt instruction](https://docs.getdbt.com/reference/node-selection/syntax#shorthand) for more details.

### Setting up (in VSCode)

1. Install Python 3.11 
2. Run `pip install poetry`
3. Clone the repository with SSH: `git clone git@github.com:TelenorNorgeInternal/s07464-mirage-dbt.git`
    - Follow [this](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent) guide for setting up SSH keys
4. Run `poetry install --no-root --with dev`
5. Enter python venv using `poetry shell`
6. Run `dbt init`
    - Choose your workspace
    - Define your schema (`dev_<your initials>`)
7. Run `dbt debug` to test your connection (might take a while if warehouse is not currently running)
8. Optional: Run `pre-commit install`
9. If everything checks out, you're all set up

### Running

Basic format is `dbt run --target <TARGET> --select <SELECTION_CRITERIA>`. Target is only relevant if 
you've installed multiple targets. See [dbt's selection syntax](https://docs.getdbt.com/reference/node-selection/syntax) for more info on `select`.

You generally will want to run using this selection syntax, as otherwise all models within all catalogs are created. Some examples:

- `dbt run --target mirage --select mirage`
- `dbt run --select mirage.staging.mobile_order`

Either using Databricks setup or VS code setup, when you execute `dbt run`, tables and views are materialized in your personal schema, such as `dev_kpark_int`, under qa catalog, such as `business_analytics_enriched_qa`. Schema name uses your initials.

### Testing

Basic format is similar to above - `dbt test --target <TARGET> --select <SELECTION_CRITERIA>`:

- `dbt test --target mirage --select mirage`


### Model & Snapshot Development Guide

[Best practices provided by dbt](https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview)

#### Models for tables & views materialization

Under the [models](./models/) folder, the models are structured into a specific hierarchy:

1. Domain (ex. `CWARP` or `busines_analytics`)
2. Model stage
    - `staging`: lightly pre-processed models (changing column names, creating buckets, etc.)
    - `intermediate`: models during active processing
    - `marts`: finished data products
    - `deliverables`: finished data products that derive directly from a staging model or source; this is for cases when ingested data in staging is already a complete data product
3. `base`/ schema name
    - Here an additional stage is added under staging - base. This is for when a staging model needs to perform joins - the models being joined fall under base
    - Otherwise, this is the level for schemas
4. Model `sql` (code) and `yaml` (metadata) files
    - These names must be globally unique within a project, and thus include information on catalog, stage, and model name: `<stage-prefix>_<schema>__<model-name>__<catalog abbreviation>.sql`

Since we customized [generate_database_name](https://docs.getdbt.com/docs/build/custom-databases#generate_database_name) and [generate_schema_name](https://docs.getdbt.com/docs/build/custom-schemas#advanced-custom-schema-configuration) macros, your models will be deployed to corret databases and schemas based on running contexts. For example,

- If you execute `dbt run` from local machine or Databricks as dbt IDE setup, your models are deployed to `_qa` catalogs and your own personal schemas, such as `dev_kpark_stg`
- When you make a PR, models are deployed to `_qa` catalogs and `dbt_pr_xx_stg` schmeas.
- With running dbt in deployment pipeline or scheduler in production, they are deployed to production database and schemas such as `stg`.

An example layout is as follows:

```
models/
├─ <domain>/
│  ├─ staging/
│  │  ├─ base/
│  │  │  ├─ <schema>/
│  │  │  │  ├─ <model>.sql
│  │  │  │  ├─ <model>.yaml
│  │  ├─ <schema>/
│  ├─ intermediate/
│  │  ├─ <schema>/
│  ├─ marts/
│  │  ├─ <schema>/
│  ├─ deliverables/
│  │  ├─ <schema>/
snapshots/
├─ <domain>/
│  ├─ marts/
│  │  ├─ <schema>/
```

-----

This translates to databricks in the following way:

1. All schemas will fall under the catalogs `<domain>_enriched` or `<domain>_curated` based on their stage:
- `<domain>_enriched`: `base`, `staging`, `ìntermediate`
- `<domain>_curated`: `marts`, `deliverables`
- e.g. models under `cwarp/intermediate` will be placed in catalog `cwarp_enriched`

2. All schema names will be prepended with their stage:
- `base`: `bas`
- `staging`: `stg`
- `ìntermediate`: `int`
- `marts`: `mart`
- `deliverables`: `dlv`
- e.g. models under `staging/sales_order` will be placed in schema `stg_sales_order`

3. Table name from a model will be drawn from the model name, removing the schema and catalog information:

- e.g. `int_mobile_order__churn_map__ci.sql` will become table `churn_map`

-----

#### Snapshots for historization

As you want to historize your data, such as SCD type2, you can make [snapshots](https://docs.getdbt.com/docs/build/snapshots). Snapshots should stay under the [snapshots](./snapshots) folder with the same structure with models, such as `<domain>/<model_stage>/<snapshot_name>.sql & .yml`. For example, `snapshots/business_analytics/marts/cust_nbr_sub_scd2.sql & .yml`.

Similarly with models, snapshots are deployed to correct databases automatically. But you need to specify schema in every snapshots configs. For example, if you want to deploy your snapshot to `mart` schema, specify this as an input of `generate_schema_name` macros in `target_schema` config, like:

```
{% snapshot cust_nbr_sub_scd2 %}

    {{
        config (
          target_schema=generate_schema_name('mart', this),
          other configs...
        )
    }}

    select * from {{ ref('stg_cust_nbr_sub_current') }}

{% endsnapshot %}
```

An example layout is as follows:

```
snapshots/
├─ <domain>/
│  ├─ marts/
│  │  ├─ <schema>/
```

-----


### CI/CD - outdated

![CI/CD](image.png)

CI/CD runs separately for each workspace. This means that all catalogs under workspace `mirage` will run on a cluster within the workspace.

It currently only runs on merge, running the same `dbt run` command as done locally, but removing the individualized prefix and thus forming the authoritative state of the data warehouse.

Being looked into is the possibility of:

1. Running certain processes on pull requests to automatically test before new changes are merged.

2. Slim CI, which only runs the changed models rather than all models in the project.