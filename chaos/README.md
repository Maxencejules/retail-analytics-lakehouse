# Chaos Engineering Experiments

This folder contains chaos experiment tooling for resilience drills across Airflow and Spark.

## Supported Experiments

- `airflow_network_partition`: isolate an Airflow container from its Docker network.
- `spark_node_crash`: terminate Spark container and observe job/orchestration recovery.

## Script

- Runner: `chaos/run_experiment.py`
- Output: JSON execution reports under `chaos/results/`

## Local Docker Drills

Airflow network partition (60s):

```bash
python chaos/run_experiment.py \
  --experiment airflow_network_partition \
  --mode local-docker \
  --duration-seconds 60 \
  --target-container airflow-webserver \
  --docker-network retail-analytics-lakehouse_default
```

Spark node crash (60s):

```bash
python chaos/run_experiment.py \
  --experiment spark_node_crash \
  --mode local-docker \
  --duration-seconds 60
```

## Gremlin Integration Mode

Use `--mode gremlin-http` to submit experiments to a Gremlin-compatible endpoint.

Airflow partition example:

```bash
python chaos/run_experiment.py \
  --experiment airflow_network_partition \
  --mode gremlin-http \
  --duration-seconds 120 \
  --target-container airflow-webserver \
  --gremlin-endpoint "$GREMLIN_API_ENDPOINT" \
  --payload-file chaos/payloads/airflow_network_partition.gremlin.example.json \
  --header "Authorization=Bearer $GREMLIN_API_TOKEN"
```

Spark crash example:

```bash
python chaos/run_experiment.py \
  --experiment spark_node_crash \
  --mode gremlin-http \
  --duration-seconds 120 \
  --target-container spark \
  --gremlin-endpoint "$GREMLIN_API_ENDPOINT" \
  --payload-file chaos/payloads/spark_node_crash.gremlin.example.json \
  --header "Authorization=Bearer $GREMLIN_API_TOKEN"
```

Notes:

- Payload files are templates, not guaranteed Gremlin API schemas.
- Replace template fields to match your Gremlin account/API contract.
- Use `--dry-run` before executing real attacks.
