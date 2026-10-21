"""Fetch the pinned Sedona runtime at image build time, never during a job."""

import hashlib
from pathlib import Path
from urllib.request import urlopen

JARS = [
    (
        "org/apache/sedona/sedona-spark-shaded-3.5_2.12/1.8.0/"
        "sedona-spark-shaded-3.5_2.12-1.8.0.jar",
        "8f393fc4d1c7f4dfd894ea44fe02011664f337ae9a8d7e1d516afd99cfb90cec",
    ),
    (
        "org/datasyslab/geotools-wrapper/1.8.0-33.1/geotools-wrapper-1.8.0-33.1.jar",
        "64d7b8296ded315e97fe90af1d40be6c24203603d952d06a093ef3470fa4cfca",
    ),
]

if __name__ == "__main__":
    destination = Path("/opt/sedona-jars")
    destination.mkdir(exist_ok=True)
    for artifact, expected in JARS:
        url = "https://repo.maven.apache.org/maven2/" + artifact
        with urlopen(url, timeout=120) as response:
            content = response.read()
        if hashlib.sha256(content).hexdigest() != expected:
            raise RuntimeError(f"Checksum mismatch: {artifact}")
        (destination / artifact.rsplit("/", 1)[1]).write_bytes(content)
        print(artifact, "sha256=" + hashlib.sha256(content).hexdigest(), flush=True)
