#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

python3 - <<'PY'
from pathlib import Path
import re
import subprocess
import xml.etree.ElementTree as ET

ns = {"m": "http://maven.apache.org/POM/4.0.0"}
root = ET.parse("pom.xml").getroot()
client = ET.parse("jms-client/pom.xml").getroot()

def value(element, path):
    result = element.findtext(path, namespaces=ns)
    if not result or not result.strip():
        raise SystemExit(f"Missing POM value: {path}")
    return result.strip()

version = value(root, "m:properties/m:revision")
if not re.fullmatch(r"2\.\d+\.\d+(?:-[A-Za-z0-9.-]+)?", version):
    raise SystemExit(f"Expected a 2.x library version, got {version!r}")

attributes = {
    "project-version": version,
    "project-group-id": value(root, "m:groupId"),
    "project-artifact-id": value(client, "m:artifactId"),
    "java-version": value(root, "m:properties/m:maven.compiler.release"),
    "source-revision": subprocess.check_output(
        ["git", "rev-parse", "HEAD"], text=True
    ).strip(),
}
target = Path("docs/target")
target.mkdir(parents=True, exist_ok=True)
(target / "library-attributes.adoc").write_text(
    "".join(f":{key}: {value}\n" for key, value in attributes.items()), encoding="utf-8"
)
(target / "library-version.txt").write_text(version + "\n", encoding="utf-8")
PY

./mvnw -B -f docs/pom.xml generate-resources --no-transfer-progress
