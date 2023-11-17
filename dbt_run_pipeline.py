import dlt

def dbt_run():

    pipeline = dlt.pipeline(
        pipeline_name='dbt_pipeline',
        destination='bigquery',
        dataset_name='dbt'
    )

    venv = dlt.dbt.get_venv(pipeline)

    dbt = dlt.dbt.package(pipeline, "dbt/property_analytics", venv=venv)

    models = dbt.run_all()

    # show outcome
    for m in models:
        print(f"Model {m.model_name} materialized in {m.time} with status {m.status} and message {m.message}")
