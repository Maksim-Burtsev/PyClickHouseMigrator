"""Wait until both nodes of the docker-compose test cluster see ``test_cluster``.

Run by ``make cluster-wait`` before the tests marked ``cluster``.
"""

import sys
import time

from clickhouse_driver import Client

NODE_URLS = (
    "clickhouse://default@localhost:19001/test",
    "clickhouse://default@localhost:19002/test",
)
CLUSTER_NAME = "test_cluster"
EXPECTED_REPLICAS = 2
MAX_ATTEMPTS = 30
SLEEP_SECONDS = 2


def main() -> int:
    """Poll the cluster; return 0 once it is ready, or 1 after ``MAX_ATTEMPTS`` tries."""
    for attempt in range(1, MAX_ATTEMPTS + 1):
        if _cluster_is_ready():
            sys.stdout.write("Cluster ready\n")
            return 0
        sys.stdout.write(f"Waiting for cluster... ({attempt}/{MAX_ATTEMPTS})\n")
        time.sleep(SLEEP_SECONDS)

    sys.stderr.write("Cluster did not become ready in time.\n")
    return 1


def _cluster_is_ready() -> bool:
    try:
        return all(_node_is_ready(url) for url in NODE_URLS)
    except Exception:
        return False


def _node_is_ready(url: str) -> bool:
    with Client.from_url(url) as client:
        if client.execute("SELECT 1") != [(1,)]:
            return False
        rows = client.execute(
            "SELECT count() FROM system.clusters WHERE cluster = %(cluster)s",
            {"cluster": CLUSTER_NAME},
        )
        return bool(rows == [(EXPECTED_REPLICAS,)])


if __name__ == "__main__":
    sys.exit(main())
