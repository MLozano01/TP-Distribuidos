import subprocess
import time

from results import comparer


def loop_client():
  while True:
    # docker compose --profile clients -f docker-compose.yaml up -d

    subprocess.run(['docker', 'compose', "--profile", "clients", "-f", "docker-compose.yaml", "up", "client1"], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    comparer.run_comparison("results/result_demo.json", "data/results_client1.json")
    time.sleep(2)


loop_client()