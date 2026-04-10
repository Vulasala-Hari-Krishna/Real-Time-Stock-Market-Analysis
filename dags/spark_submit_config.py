"""Shared spark-submit configuration for batch DAGs.

All Spark batch jobs are submitted to the standalone Spark cluster
via ``spark-submit``.  This module centralises the master URL and
package dependencies so every DAG uses the same settings.
"""

SPARK_MASTER = "spark://spark-master:7077"

SPARK_PACKAGES = ",".join(
    [
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        "io.delta:delta-spark_2.12:3.2.1",
    ]
)

DELTA_EXTENSIONS = (
    "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
    "--conf spark.sql.catalog.spark_catalog="
    "org.apache.spark.sql.delta.catalog.DeltaCatalog"
)


def spark_submit_cmd(application: str, extra_args: str = "") -> str:
    """Build a ``spark-submit`` command string for BashOperator.

    Args:
        application: Absolute path to the PySpark script inside the
            Airflow container (e.g. ``/opt/airflow/src/batch/xxx.py``).
        extra_args: Optional CLI arguments appended after the script
            path (e.g. ``--mode daily``).

    Returns:
        Full ``spark-submit`` command ready for BashOperator.
    """
    cmd = (
        f"spark-submit "
        f"--master {SPARK_MASTER} "
        f"--packages {SPARK_PACKAGES} "
        f"{DELTA_EXTENSIONS} "
        f"--conf spark.pyspark.python=python3 "
        f"--conf spark.pyspark.driver.python=python3 "
        f"{application}"
    )
    if extra_args:
        cmd += f" {extra_args}"
    return cmd
