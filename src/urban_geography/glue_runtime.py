"""Render a pinned Glue startup recipe without starting Spark or contacting AWS."""

import re
from typing import Any

# Cloud runtime pins intentionally match the existing local build pins.
JARS = [
    (
        "org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.10.2/"
        "iceberg-spark-runtime-3.5_2.12-1.10.2.jar",
        "c562d482dcd16b666e808b755469fbc4b2604284b41622c9d87e2c02b7cc6c5f",
    ),
    (
        "org/apache/sedona/sedona-spark-shaded-3.5_2.12/1.8.0/"
        "sedona-spark-shaded-3.5_2.12-1.8.0.jar",
        "8f393fc4d1c7f4dfd894ea44fe02011664f337ae9a8d7e1d516afd99cfb90cec",
    ),
    (
        "org/datasyslab/geotools-wrapper/1.8.0-33.1/geotools-wrapper-1.8.0-33.1.jar",
        "64d7b8296ded315e97fe90af1d40be6c24203603d952d06a093ef3470fa4cfca",
    ),
    (
        "org/apache/iceberg/iceberg-aws-bundle/1.10.2/iceberg-aws-bundle-1.10.2.jar",
        "a2395ab49bf87e971e5588db644194ac18e9c0e0b44a7ad5a422a97648a38415",
    ),
]


def runtime_plan(
    *, deployment_name: str, account_id: str, region: str = "eu-west-1"
) -> dict[str, Any]:
    """Use the reviewed Terraform naming contract; values cannot inject Spark options."""
    if not re.fullmatch(r"[a-z][a-z0-9-]{2,18}[a-z0-9]", deployment_name):
        raise ValueError("deployment_name must contain 4..20 safe lowercase characters")
    if not re.fullmatch(r"[0-9]{12}", account_id):
        raise ValueError("account_id must be a 12-digit AWS account ID")
    if region != "eu-west-1":
        raise ValueError("This proposed runtime is scoped to eu-west-1")
    bucket = f"{deployment_name}-lake-{account_id}-{region}"
    warehouse = f"s3://{bucket}/warehouse/"
    prefix = "spark.sql.catalog.glue_catalog"
    settings = {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.kryo.registrator": "org.apache.sedona.core.serde.SedonaKryoRegistrator",
        "spark.sql.session.timeZone": "UTC",
        prefix: "org.apache.iceberg.spark.SparkCatalog",
        prefix + ".catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
        prefix + ".io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        prefix + ".warehouse": warehouse,
        prefix + ".glue.id": account_id,
        prefix + ".client.region": region,
        prefix + ".s3.sse.type": "s3",
        prefix + ".table-override.format-version": "2",
    }
    artifacts = [
        {
            "filename": artifact.rsplit("/", 1)[1],
            "sha256": checksum,
            "source_url": "https://repo.maven.apache.org/maven2/" + artifact,
            "proposed_s3_uri": f"s3://{bucket}/runtime/jars/{artifact.rsplit('/', 1)[1]}",
        }
        for artifact, checksum in JARS
    ]
    return {
        "contract_version": 1,
        "status": "prepared_configuration_only",
        "glue_version": "5.1",
        "runtime": {
            "spark": "3.5.6",
            "python": "3.11",
            "scala": "2.12.18",
            "java": "17",
            "sedona": "1.8.0",
            "iceberg": "1.10.2",
            "iceberg_format": 2,
        },
        "catalog": "glue_catalog",
        "database": deployment_name.replace("-", "_") + "_m4",
        "warehouse": warehouse,
        "spark_settings": settings,
        "jar_artifacts": artifacts,
        "jar_job_arguments": {
            "--user-jars-first": "true",
            "--extra-jars": ",".join(item["proposed_s3_uri"] for item in artifacts),
            "--conf": " --conf ".join(f"{key}={value}" for key, value in settings.items()),
        },
        "python_requirement": "apache-sedona==1.8.0",
        "scope": (
            "Startup configuration only. No uploads, credentials, IAM, job creation or execution. "
            "Execution consumes separately prepared software artifacts and retained input."
        ),
    }
