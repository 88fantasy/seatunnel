---
title: OpenLineage Lineage Reporting
---

<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# OpenLineage Lineage Reporting

SeaTunnel can emit table-level OpenLineage `RunEvent` records from the Zeta and Flink execution
engines. The feature is disabled by default. When it is disabled, SeaTunnel does not create a
lineage network call or a lineage-specific background thread, and existing job behavior is
unchanged.

## Configuration

The following options are available in a job `env` block unless noted otherwise.

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `openlineage_enabled` | Boolean | `false` | Enables OpenLineage reporting. |
| `openlineage_transport` | String | `http` | Transport name selected from the `LineageBackend` SPI. |
| `openlineage_url` | String | None | Base URL of the OpenLineage receiver. Required when reporting is enabled. |
| `openlineage_namespace` | String | `seatunnel` | OpenLineage job namespace. |
| `openlineage_auth_token` | String | None | Receiver bearer token. It may come only from an environment variable or cluster configuration. |
| `openlineage_timeout_ms` | Int | `10000` | Timeout for one send attempt, in milliseconds. |
| `openlineage_retry_times` | Int | `3` | Number of retries after a failed send. |
| `openlineage_run_facet` | String | `seatunnel_properties` | Name of the custom run facet. |
| `openlineage_run_properties` | Map | None | Custom properties copied to the run facet. |
| `openlineage_heartbeat_min_interval_ms` | Long | `3600000` | Minimum interval between streaming-job heartbeat events, in milliseconds. |
| `openlineage_producer` | String | `https://seatunnel.apache.org/<version>` | OpenLineage producer identifier. The default is derived from the running SeaTunnel version at runtime. |

Values are resolved independently with this precedence:

```text
job env {} > process environment > cluster configuration > default
```

The token is the exception to this rule. `openlineage_auth_token` is resolved as
`OPENLINEAGE_AUTH_TOKEN` > cluster configuration. A token in a job `env {}` block is rejected at
job startup. This prevents the token from being persisted in job configuration or serialized into
the Flink JobGraph.

Lineage delivery is unconditionally best effort, and there is deliberately no option to change that.
A receiver or network failure is contained at the engine integration boundary and reported as a
warning, so it can never change the outcome of the data-processing job.

### Environment variable names

The environment variable name is the upper-case option name:

| Option | Environment variable |
| --- | --- |
| `openlineage_enabled` | `OPENLINEAGE_ENABLED` |
| `openlineage_transport` | `OPENLINEAGE_TRANSPORT` |
| `openlineage_url` | `OPENLINEAGE_URL` |
| `openlineage_namespace` | `OPENLINEAGE_NAMESPACE` |
| `openlineage_auth_token` | `OPENLINEAGE_AUTH_TOKEN` |
| `openlineage_timeout_ms` | `OPENLINEAGE_TIMEOUT_MS` |
| `openlineage_retry_times` | `OPENLINEAGE_RETRY_TIMES` |
| `openlineage_run_facet` | `OPENLINEAGE_RUN_FACET` |
| `openlineage_run_properties` | `OPENLINEAGE_RUN_PROPERTIES` |
| `openlineage_heartbeat_min_interval_ms` | `OPENLINEAGE_HEARTBEAT_MIN_INTERVAL_MS` |
| `openlineage_producer` | `OPENLINEAGE_PRODUCER` |

For example, set `OPENLINEAGE_URL` and `OPENLINEAGE_AUTH_TOKEN` through the process or service
manager environment. Do not put a real token in a job file, repository file, command history, or
logs.

### Zeta cluster configuration

Zeta reads cluster-level values from `seatunnel.yaml`:

```yaml
seatunnel:
  engine:
    openlineage:
      enabled: true
      transport: http
      url: http://lineage.example/api/lineage
      namespace: seatunnel
      timeout_ms: 10000
      retry_times: 3
      run_facet: seatunnel_properties
      heartbeat_min_interval_ms: 3600000
      producer: https://seatunnel.apache.org/<version>
```

The cluster-level `auth_token` value is supported, but should be supplied through the cluster's
secret-management mechanism. It must not be copied into a job `env` block.

### Flink cluster configuration

Flink reads cluster-level values with the `openlineage.` prefix. For Flink 1.20 and later, use
`config.yaml`; the legacy `flink-conf.yaml` name is also supported:

```yaml
openlineage.enabled: true
openlineage.transport: http
openlineage.url: http://lineage.example/api/lineage
openlineage.namespace: seatunnel
openlineage.timeout_ms: 10000
openlineage.retry_times: 3
openlineage.run_facet: seatunnel_properties
openlineage.heartbeat_min_interval_ms: 3600000
openlineage.producer: https://seatunnel.apache.org/<version>
```

The lineage backend JARs, including the lineage contract and OpenLineage backend artifacts, must be
available in the Flink cluster `lib/` directory. Keep the same cluster configuration in both
configuration-file layouts when an installation supports both Flink 1.20+ and legacy deployments.

## Events and dataset names

Zeta emits `START` when a job enters `RUNNING`, then emits `COMPLETE`, `ABORT`, or `FAIL` for the
corresponding terminal state. `SAVEPOINT_DONE` is represented as `COMPLETE`, while `UNKNOWABLE` is
represented as `FAIL`. Zeta reads terminal output metrics from historical job metrics, preferring
committed counters and falling back to attempted write counters when no positive committed counter
is available.

Flink registers the status hook only when the runtime exposes the required API. Flink lineage
support requires Flink 1.16 or later, which in the SeaTunnel starters means the Flink 1.20 path;
the Flink 1.13 and 1.15 starters do not register this hook.

Besides the properties configured through `openlineage_run_properties`, the run facet carries an
`engine` property (`zeta` or `flink`). Zeta additionally reports `sink_action`, and Flink reports
`flink_job_id` on terminal events. The run ID is derived before submission, when no Flink job ID
exists yet, so `flink_job_id` is what ties a lineage run back to the job shown in the Flink UI and
REST API.

Supported connector dataset identities use these canonical forms. For multi-table sources and sinks,
each configured table is represented separately.

| Connector | Namespace | Name |
| --- | --- | --- |
| Paimon | `paimon://<catalog_name>/<database>` | `<table>` |
| Doris | `mysql://<fe_host>:<query_port>` | `<database>.<table>` |
| JDBC | `<scheme>://<host>:<port>` | `<database>.<table>` |

For Paimon, `<catalog_name>` comes from the connector's `catalog_name` option and the dataset name is
the table name only, not `database.table`. Set `catalog_name` explicitly to keep the namespace stable
across jobs; when it is omitted, no Paimon dataset is emitted. For Doris, `fenodes` normally contains the HTTP Stream Load port; the
lineage namespace uses the Doris query port instead. The generic `default.default.default` table path
is not emitted.

## Known limitations

1. Zeta and Flink use different output-count semantics. Zeta prefers `committed` values and falls
   back to `attempted`; Flink reports `attempted` values. Counts can therefore differ when the same
   workload is moved between engines.
2. Flink lineage requires Flink 1.16 or later. In SeaTunnel, the supported path is the
   `flink-20-starter`; the Flink 1.13 and 1.15 starters do not provide lineage reporting.
3. A detached Flink job has no `outputStatistics`, because its client result does not expose
   accumulators. Attached execution can emit the available `SinkWriteCount` and `SinkWriteBytes`
   values, with `attempted` semantics.
4. Streaming jobs receive heartbeat events only when checkpointing is enabled. With checkpointing
   disabled, the lineage edge is subject to the receiver's first 24-hour abandoned-run timeout.
   Checkpoint heartbeats do not extend the receiver's second, seven-day absolute lifetime limit for
   an uncompleted run. A continuously running run can still be marked `ABANDONED` after seven days;
   a later terminal event can replace that inferred state.
5. The Flink lineage contract and backend JARs must be installed in the cluster `lib/` directory.
   Flink 1.20+ uses `config.yaml`, while legacy deployments use `flink-conf.yaml`; configure the
   `openlineage.` values in the file layout used by each deployment. If both layouts are maintained,
   keep the corresponding configuration in both files.

Lineage delivery is always best effort at the engine boundary. A receiver or network failure is
contained and logged as a warning, and no configuration option can make it fail the job.
