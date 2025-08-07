Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices


### Setup for Python model

Using BigQuery Dataframes for dbt python model causes too many errors. This is because this option is still in beta.

Instead, Dataproc is used. See here for dbt setup for this - https://docs.getdbt.com/reference/resource-configs/bigquery-configs#python-model-configuration

For using this, we need a custom Docker image. See /py_model for docker file.

Useful commands:
```
docker build -t europe-north1-docker.pkg.dev/xnwk-462111/dbt/python_model:v1 .
gcloud auth configure-docker europe-north1-docker.pkg.dev
docker push europe-north1-docker.pkg.dev/xnwk-462111/dbt/python_model:v1

```

