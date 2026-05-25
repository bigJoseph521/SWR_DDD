# Build from repository root:
#   docker build -f services/python/strategy-worker-runtime/Dockerfile -t strategy-worker-runtime .
#
# WORKDIR /app: launch identity from SDS runtime-context (DEPLOYMENT_ID + STRATEGY_DEPLOYMENT_SERVICE_BASE_URL)
# or from strategy_bundle/setting.json when those are unset.

FROM python:3.11.9-slim-bookworm

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
      build-essential \
      gcc \
      libcairo2-dev \
      pkg-config \
    && rm -rf /var/lib/apt/lists/*

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY services/python/strategy-worker-runtime/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --upgrade pip setuptools wheel \
    && pip install --no-cache-dir -r /tmp/requirements.txt

# Build/install SDK under /tmp only — do not leave a source tree under /app (WORKDIR).
# Otherwise Python prepends cwd (/app) to sys.path and may import incomplete /app/alphovex_sdk
# instead of the pip-installed package (pods then fail while `docker run … python -c` can succeed).
COPY services/python/strategy-worker-runtime/alphovex_sdk /tmp/alphovex_sdk

# Prefer the cloned SDK's pyproject.toml when present; synthesize one only for legacy trees.
RUN python <<'PY'
from pathlib import Path
import textwrap

root = Path("/tmp/alphovex_sdk")
if not (root / "__init__.py").is_file():
    raise SystemExit("alphovex_sdk: missing __init__.py")
if not (root / "context" / "__init__.py").is_file():
    raise SystemExit("alphovex_sdk: missing context package")

if (root / "pyproject.toml").is_file():
    print("alphovex_sdk: using existing pyproject.toml")
else:

    def subpackages(base: Path, prefix: str) -> list[str]:
        names: list[str] = []
        for p in sorted(base.iterdir()):
            if p.is_dir() and (p / "__init__.py").is_file():
                qn = f"{prefix}.{p.name}"
                names.append(qn)
                names.extend(subpackages(p, qn))
        return names

    pkgs = ["alphovex_sdk"] + subpackages(root, "alphovex_sdk")
    body = textwrap.dedent(
        """\
        [build-system]
        requires = ["setuptools>=61", "wheel"]
        build-backend = "setuptools.build_meta"

        [project]
        name = "alphovex-sdk"
        version = "0.1.0"
        requires-python = ">=3.11"

        [tool.setuptools.package-dir]
        alphovex_sdk = "."

        [tool.setuptools]
        packages = {packages}
        """
    ).replace("{packages}", repr(pkgs))
    root.joinpath("pyproject.toml").write_text(body, encoding="utf-8")
    print("alphovex_sdk: generated pyproject.toml")
PY

RUN pip install --no-cache-dir /tmp/alphovex_sdk \
    && python -c "from alphovex_sdk.context.indicator_context import DataSourceEnum; from alphovex_sdk import StrategyContext; DataSourceEnum; StrategyContext" \
    && rm -rf /tmp/alphovex_sdk

# In-repo package is ``runtime/``; image exposes ``strategy_worker_runtime`` for K8s/SRM pods.
COPY services/python/strategy-worker-runtime/runtime /app/src/strategy_worker_runtime
RUN python <<'PY'
from pathlib import Path

root = Path("/app/src/strategy_worker_runtime")
for path in root.rglob("*.py"):
    text = path.read_text(encoding="utf-8")
    new = text.replace("from runtime.", "from strategy_worker_runtime.")
    if new != text:
        path.write_text(new, encoding="utf-8")
PY
COPY services/python/strategy-worker-runtime/migrations /app/src/migrations
COPY services/python/strategy-worker-runtime/protos /app/protos
COPY services/python/strategy-worker-runtime/strategy_bundle /app/strategy_bundle

ENV PYTHONSAFEPATH=1
ENV PYTHONPATH=/app/src:/app/protos/generated

CMD ["python", "-m", "strategy_worker_runtime.main"]
