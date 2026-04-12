from __future__ import annotations

import sys
import time

from clickhouse_driver import Client

NODE_URLS = {
    "clickhouse-01": "clickhouse://default@localhost:19001/test",
    "clickhouse-02": "clickhouse://default@localhost:19002/test",
}
CLUSTER_NAME = "test_cluster"
EXPECTED_REPLICAS = 2
MAX_ATTEMPTS = 30
SLEEP_SECONDS = 2


def _is_ready() -> bool:
    for url in NODE_URLS.values():
        client = Client.from_url(url)
        try:
            if client.execute("SELECT 1") != [(1,)]:
                return False
            rows = client.execute(
                "SELECT count() FROM system.clusters WHERE cluster = %(cluster)s",
                {"cluster": CLUSTER_NAME},
            )
            if rows != [(EXPECTED_REPLICAS,)]:
                return False
        finally:
            if client.connection.connected:
                client.disconnect()
    return True


def main() -> int:
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            if _is_ready():
                print("Cluster ready")
                return 0
        except Exception:
            pass

        print(f"Waiting for cluster... ({attempt}/{MAX_ATTEMPTS})")
        time.sleep(SLEEP_SECONDS)

    print("Cluster did not become ready in time.", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
