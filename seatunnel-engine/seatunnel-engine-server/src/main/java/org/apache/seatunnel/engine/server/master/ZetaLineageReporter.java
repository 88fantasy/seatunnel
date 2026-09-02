/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.server.master;

import org.apache.seatunnel.api.common.metrics.JobMetrics;
import org.apache.seatunnel.api.common.metrics.Measurement;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.multitablesink.MultiTableSink;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.dag.logical.LogicalVertex;
import org.apache.seatunnel.lineage.LineageConfig;
import org.apache.seatunnel.lineage.LineageDataset;
import org.apache.seatunnel.lineage.LineageEvent;
import org.apache.seatunnel.lineage.LineageEventType;
import org.apache.seatunnel.lineage.LineageOutputStatistics;
import org.apache.seatunnel.lineage.LineageReporter;
import org.apache.seatunnel.lineage.LineageReporterFactory;
import org.apache.seatunnel.lineage.LineageRunIds;
import org.apache.seatunnel.lineage.LineageRuntime;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/** Builds Zeta lineage events from the restored logical DAG and job metrics. */
public final class ZetaLineageReporter {
    private static final ILogger LOGGER = Logger.getLogger(ZetaLineageReporter.class);
    private static final int JOB_HEARTBEAT_KEY = Integer.MIN_VALUE;

    private ZetaLineageReporter() {}

    /**
     * Reports a Zeta job lifecycle event when the job enters a lineage-relevant state.
     *
     * @param jobMaster job master that owns the logical DAG and job configuration
     * @param status current job status
     */
    public static void reportJobState(JobMaster jobMaster, JobStatus status) {
        if (status != JobStatus.RUNNING && !status.isEndState()) {
            return;
        }
        LineageConfig config = jobMaster.resolveLineageConfig();
        if (!config.enabled()) {
            return;
        }
        LineageEventType eventType = eventType(status);
        boolean includeStatistics = eventType == LineageEventType.COMPLETE;
        emit(config, events(jobMaster, config, eventType, includeStatistics), eventType);
    }

    /**
     * Reports a throttled lineage heartbeat after a completed streaming checkpoint.
     *
     * @param jobMaster job master that owns the pipeline
     * @param pipelineId completed checkpoint pipeline identifier
     */
    public static void reportHeartbeat(JobMaster jobMaster, int pipelineId) {
        try {
            LineageConfig config = jobMaster.resolveLineageConfig();
            if (!config.enabled() || jobMaster.getJobStatus().isEndState()) {
                return;
            }
            if (jobMaster.getJobImmutableInformation().getJobConfig().getJobContext().getJobMode()
                    != JobMode.STREAMING) {
                return;
            }
            if (!shouldReportHeartbeat(jobMaster, pipelineId, config.heartbeatMinIntervalMs())) {
                return;
            }
            emit(
                    config,
                    events(jobMaster, config, LineageEventType.RUNNING, true),
                    LineageEventType.RUNNING);
        } catch (Throwable error) {
            LOGGER.warning("Failed to emit Zeta lineage heartbeat", error);
        }
    }

    private static boolean shouldReportHeartbeat(
            JobMaster jobMaster, int ignoredPipelineId, long minimumIntervalMs) {
        Map<Integer, AtomicLong> heartbeatTimes = jobMaster.getLineageHeartbeatTimes();
        // Checkpoint callbacks carry a pipeline id, but lineage heartbeat expiry is job-wide.
        AtomicLong lastTime =
                heartbeatTimes.computeIfAbsent(JOB_HEARTBEAT_KEY, ignored -> new AtomicLong());
        long now = System.currentTimeMillis();
        while (true) {
            long previous = lastTime.get();
            if (previous != 0 && now - previous < minimumIntervalMs) {
                return false;
            }
            if (lastTime.compareAndSet(previous, now)) {
                return true;
            }
        }
    }

    private static List<LineageEvent> events(
            JobMaster jobMaster,
            LineageConfig config,
            LineageEventType eventType,
            boolean includeStatistics) {
        LogicalDag logicalDag = jobMaster.getLogicalDag();
        if (logicalDag == null) {
            return Collections.emptyList();
        }
        JobMetrics metrics = includeStatistics ? outputMetrics(jobMaster, eventType) : null;
        List<LineageEvent> events = new ArrayList<>();
        for (LogicalVertex vertex : logicalDag.getLogicalVertexMap().values()) {
            Action action = vertex.getAction();
            if (!(action instanceof SinkAction)) {
                continue;
            }
            SinkAction<?, ?, ?, ?> sinkAction = (SinkAction<?, ?, ?, ?>) action;
            List<LineageDataset> inputs = sourceDatasets(sinkAction);
            for (LineageDataset output : sinkDatasets(sinkAction)) {
                LineageDataset eventOutput = output;
                if (includeStatistics && eventType != LineageEventType.FAIL) {
                    LineageOutputStatistics statistics =
                            outputStatistics(metrics, sinkAction, output);
                    if (statistics != null) {
                        eventOutput = output.withOutputStatistics(statistics);
                    }
                }
                Map<String, Object> properties = new HashMap<>(config.runProperties());
                properties.put("engine", "zeta");
                properties.put("sink_action", sinkAction.getName());
                events.add(
                        LineageEvent.builder()
                                .runId(
                                        LineageRunIds.forJob(
                                                jobMaster.getJobId(),
                                                output.namespace(),
                                                output.name(),
                                                jobMaster.getLineageAttempt()))
                                .eventTime(ZonedDateTime.now(ZoneOffset.UTC))
                                .eventType(eventType)
                                .jobNamespace(config.namespace())
                                .jobName(
                                        jobMaster
                                                .getJobImmutableInformation()
                                                .getJobConfig()
                                                .getName())
                                .producer(config.producer())
                                .runFacet(config.runFacet())
                                .runProperties(properties)
                                .inputs(inputs)
                                .outputs(Collections.singletonList(eventOutput))
                                .build());
            }
        }
        return events;
    }

    private static void emit(
            LineageConfig config, List<LineageEvent> events, LineageEventType eventType) {
        if (events.isEmpty()) {
            return;
        }
        try {
            LineageReporter reporter = LineageReporterFactory.create(config);
            for (LineageEvent event : events) {
                if (eventType == LineageEventType.START) {
                    reporter.start(event);
                } else if (eventType == LineageEventType.RUNNING) {
                    reporter.heartbeat(event);
                } else if (eventType == LineageEventType.COMPLETE) {
                    reporter.complete(event);
                } else {
                    LineageRuntime.emit(config, event);
                }
            }
        } catch (Throwable error) {
            LOGGER.warning("Failed to emit Zeta lineage event", error);
        }
    }

    private static LineageEventType eventType(JobStatus status) {
        switch (status) {
            case FINISHED:
            case SAVEPOINT_DONE:
                return LineageEventType.COMPLETE;
            case CANCELED:
                return LineageEventType.ABORT;
            case FAILED:
            case UNKNOWABLE:
                return LineageEventType.FAIL;
            case RUNNING:
                return LineageEventType.START;
            default:
                throw new IllegalArgumentException("Unsupported lineage job status: " + status);
        }
    }

    private static List<LineageDataset> sourceDatasets(Action action) {
        Set<LineageDataset> datasets = new LinkedHashSet<>();
        collectSourceDatasets(action, Collections.newSetFromMap(new IdentityHashMap<>()), datasets);
        return new ArrayList<>(datasets);
    }

    private static void collectSourceDatasets(
            Action action, Set<Action> visited, Set<LineageDataset> datasets) {
        if (!visited.add(action)) {
            return;
        }
        if (action instanceof SourceAction) {
            datasets.addAll(((SourceAction<?, ?, ?>) action).getLineageDatasets());
            return;
        }
        for (Action upstream : action.getUpstream()) {
            collectSourceDatasets(upstream, visited, datasets);
        }
    }

    private static List<LineageDataset> sinkDatasets(SinkAction<?, ?, ?, ?> action) {
        List<LineageDataset> configured = action.getLineageDatasets();
        if (!(action.getSink() instanceof MultiTableSink)) {
            return configured;
        }

        // MultiTableSink intentionally returns no write CatalogTable; inspect its actual sink map.
        Map<TablePath, SeaTunnelSink> sinks = ((MultiTableSink) action.getSink()).getSinks();
        if (sinks.isEmpty()) {
            return Collections.emptyList();
        }
        List<LineageDataset> matched = new ArrayList<>();
        for (TablePath tablePath : sinks.keySet()) {
            configured.stream()
                    .filter(dataset -> matchesTablePath(tablePath, dataset))
                    .findFirst()
                    .ifPresent(matched::add);
        }
        return matched;
    }

    private static JobMetrics outputMetrics(JobMaster jobMaster, LineageEventType eventType) {
        if (eventType == LineageEventType.RUNNING) {
            return jobMaster.getCurrentJobMetricsForLineage();
        }
        return jobMaster.getJobHistoryService().getJobMetrics(jobMaster.getJobId());
    }

    private static LineageOutputStatistics outputStatistics(
            JobMetrics metrics, SinkAction<?, ?, ?, ?> sinkAction, LineageDataset output) {
        List<TablePath> tablePaths = metricTablePaths(sinkAction);
        Long committedCount = metricValue(metrics, "SinkCommittedCount", output, tablePaths);
        Long committedBytes = metricValue(metrics, "SinkCommittedBytes", output, tablePaths);
        if (committedCount != null && committedCount > 0) {
            return new LineageOutputStatistics(committedCount, committedBytes, "committed");
        }
        Long writeCount = metricValue(metrics, "SinkWriteCount", output, tablePaths);
        Long writeBytes = metricValue(metrics, "SinkWriteBytes", output, tablePaths);
        if (writeCount != null && writeCount > 0) {
            return new LineageOutputStatistics(writeCount, writeBytes, "attempted");
        }
        return null;
    }

    private static List<TablePath> metricTablePaths(SinkAction<?, ?, ?, ?> action) {
        if (action.getSink() instanceof MultiTableSink) {
            return ((MultiTableSink) action.getSink())
                    .getSinks().keySet().stream().collect(Collectors.toList());
        }
        if (action.getConfig() != null) {
            return Collections.singletonList(action.getConfig().getTablePath());
        }
        return Collections.emptyList();
    }

    private static Long metricValue(
            JobMetrics metrics,
            String metricName,
            LineageDataset output,
            List<TablePath> tablePaths) {
        for (TablePath tablePath : tablePaths) {
            if (!matchesTablePath(tablePath, output)) {
                continue;
            }
            Long value = sum(metrics, metricName + "#" + tablePath.getFullName());
            if (value != null) {
                return value;
            }
        }
        if (tablePaths.size() <= 1) {
            Long global = sum(metrics, metricName);
            if (global != null) {
                return global;
            }
        }
        return null;
    }

    /**
     * Matches a catalog table path against a dataset derived from connector options.
     *
     * <p>Paimon carries the database in the namespace rather than in the dataset name, so it is
     * matched on database plus table.
     *
     * <p>For every other connector the dataset name is {@code database.table}, while {@link
     * TablePath#getFullName()} inserts a schema segment when the catalog has one (PostgreSQL,
     * Oracle). Comparing only against the full name would therefore never match a {@code
     * database.schema.table} path, and the sink would silently report no lineage at all, so the
     * database-and-table form is compared as well.
     */
    private static boolean matchesTablePath(TablePath tablePath, LineageDataset dataset) {
        if (dataset.namespace().startsWith("paimon://")) {
            int separator = dataset.namespace().lastIndexOf('/');
            return separator >= 0
                    && tablePath.getDatabaseName() != null
                    && tablePath
                            .getDatabaseName()
                            .equals(dataset.namespace().substring(separator + 1))
                    && tablePath.getTableName().equals(dataset.name());
        }
        String fullName = tablePath.getFullName();
        if (fullName.equals(dataset.name()) || fullName.equals(dataset.tablePath())) {
            return true;
        }
        if (tablePath.getSchemaName() == null || tablePath.getDatabaseName() == null) {
            return false;
        }
        String databaseAndTable = tablePath.getDatabaseName() + "." + tablePath.getTableName();
        return databaseAndTable.equals(dataset.name())
                || databaseAndTable.equals(dataset.tablePath());
    }

    private static Long sum(JobMetrics metrics, String metricName) {
        List<Measurement> measurements = metrics.get(metricName);
        if (measurements.isEmpty()) {
            return null;
        }
        long total = 0;
        boolean hasNumber = false;
        for (Measurement measurement : measurements) {
            if (measurement.value() instanceof Number) {
                total += ((Number) measurement.value()).longValue();
                hasNumber = true;
            }
        }
        return hasNumber ? total : null;
    }
}
