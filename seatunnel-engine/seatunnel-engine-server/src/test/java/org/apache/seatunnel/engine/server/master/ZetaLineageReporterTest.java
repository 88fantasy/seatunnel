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
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.RestoreMode;
import org.apache.seatunnel.lineage.LineageConfig;
import org.apache.seatunnel.lineage.LineageDataset;
import org.apache.seatunnel.lineage.LineageEventType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class ZetaLineageReporterTest {

    @Test
    void shouldUseDistinctAttemptsForRestartAndCheckpointRestores() throws Exception {
        JobMaster jobMaster = mock(JobMaster.class, CALLS_REAL_METHODS);
        setField(jobMaster, "initializationTimestamp", 42L);

        JobImmutableInformation savepointInfo = mock(JobImmutableInformation.class);
        doReturn(true).when(savepointInfo).isRestoreJob();
        doReturn(RestoreMode.SAVEPOINT).when(savepointInfo).getRestoreMode();
        setField(jobMaster, "jobImmutableInformation", savepointInfo);
        setField(jobMaster, "restart", false);
        String savepointAttempt = jobMaster.getLineageAttempt();

        JobImmutableInformation checkpointInfo = mock(JobImmutableInformation.class);
        doReturn(true).when(checkpointInfo).isRestoreJob();
        doReturn(RestoreMode.CHECKPOINT).when(checkpointInfo).getRestoreMode();
        setField(jobMaster, "jobImmutableInformation", checkpointInfo);
        String checkpointAttempt = jobMaster.getLineageAttempt();

        setField(jobMaster, "restart", true);
        String restartAttempt = jobMaster.getLineageAttempt();

        Assertions.assertNotNull(savepointAttempt);
        Assertions.assertNotNull(checkpointAttempt);
        Assertions.assertNotNull(restartAttempt);
        Assertions.assertNotEquals(savepointAttempt, checkpointAttempt);
        Assertions.assertNotEquals(savepointAttempt, restartAttempt);
        Assertions.assertNotEquals(checkpointAttempt, restartAttempt);
    }

    /**
     * The restart attempt must be a pure function of the initialization timestamp so that a run ID
     * can be recomputed offline from job facts. A random discriminator would satisfy uniqueness but
     * would make the run ID unrecoverable outside the emitted events.
     */
    @Test
    void shouldDeriveRecomputableRestartAttemptFromInitializationTimestamp() throws Exception {
        JobMaster jobMaster = mock(JobMaster.class, CALLS_REAL_METHODS);
        setField(jobMaster, "restart", true);

        setField(jobMaster, "initializationTimestamp", 42L);
        String firstAttempt = jobMaster.getLineageAttempt();
        String recomputedAttempt = jobMaster.getLineageAttempt();

        setField(jobMaster, "initializationTimestamp", 43L);
        String laterAttempt = jobMaster.getLineageAttempt();

        Assertions.assertEquals("restart-42", firstAttempt);
        Assertions.assertEquals(firstAttempt, recomputedAttempt);
        Assertions.assertEquals("restart-43", laterAttempt);
        Assertions.assertNotEquals(firstAttempt, laterAttempt);
    }

    @Test
    void shouldResolveDisabledConfigWhenEngineConfigIsAbsent() throws Exception {
        JobMaster jobMaster = mock(JobMaster.class, CALLS_REAL_METHODS);
        JobImmutableInformation jobInformation = mock(JobImmutableInformation.class);
        doReturn(new JobConfig()).when(jobInformation).getJobConfig();
        setField(jobMaster, "jobImmutableInformation", jobInformation);

        LineageConfig config = Assertions.assertDoesNotThrow(jobMaster::resolveLineageConfig);

        Assertions.assertFalse(config.enabled());
    }

    @Test
    void shouldUseCurrentMetricsForHeartbeatAndHistoryForTerminalEvent() throws Exception {
        JobMetrics history = metrics("history");
        JobMetrics current = metrics("current");

        Assertions.assertSame(
                current,
                invokeOutputMetrics(
                        mockCurrentMetricsJobMaster(current), LineageEventType.RUNNING));
        Assertions.assertSame(
                history,
                invokeOutputMetrics(
                        mockHistoryMetricsJobMaster(history), LineageEventType.COMPLETE));
    }

    @Test
    void shouldCollectCurrentMetricsWithoutPersistingOrCleaning() {
        JobMaster jobMaster = mock(JobMaster.class, CALLS_REAL_METHODS);
        doReturn(Collections.emptyList()).when(jobMaster).getCurrJobMetrics();

        JobMetrics current = jobMaster.getCurrentJobMetricsForLineage();

        Assertions.assertTrue(current.metrics().isEmpty());
        verify(jobMaster).getCurrJobMetrics();
        verify(jobMaster, never()).savePipelineMetricsToHistory(org.mockito.ArgumentMatchers.any());
    }

    @Test
    void shouldThrottleHeartbeatsAcrossPipelinesForOneJob() throws Exception {
        JobMaster jobMaster = mock(JobMaster.class, CALLS_REAL_METHODS);
        doReturn(new ConcurrentHashMap<Integer, AtomicLong>())
                .when(jobMaster)
                .getLineageHeartbeatTimes();

        Assertions.assertTrue(invokeShouldReportHeartbeat(jobMaster, 1, Long.MAX_VALUE));
        Assertions.assertFalse(invokeShouldReportHeartbeat(jobMaster, 2, Long.MAX_VALUE));
    }

    @Test
    void shouldMatchMultiTableDatasetByExactTablePath() throws Exception {
        LineageDataset firstPaimonTable = LineageDataset.of("paimon://catalog/db1", "orders");
        LineageDataset secondPaimonTable = LineageDataset.of("paimon://catalog/db2", "orders");
        LineageDataset firstJdbcTable = LineageDataset.of("mysql://host:3306", "db1.orders");

        Assertions.assertTrue(
                invokeMatchesTablePath(TablePath.of("db1", "orders"), firstPaimonTable));
        Assertions.assertFalse(
                invokeMatchesTablePath(TablePath.of("db2", "orders"), firstPaimonTable));
        Assertions.assertTrue(
                invokeMatchesTablePath(TablePath.of("db2", "orders"), secondPaimonTable));
        Assertions.assertTrue(
                invokeMatchesTablePath(TablePath.of("db1", "orders"), firstJdbcTable));
        Assertions.assertFalse(
                invokeMatchesTablePath(TablePath.of("db2", "orders"), firstJdbcTable));
    }

    /**
     * A catalog whose tables carry a schema (PostgreSQL, Oracle) produces a {@code
     * database.schema.table} path, while the connector-level dataset name is {@code
     * database.table}. Comparing only the full name would never match, and the sink would report no
     * lineage at all without any error, so both forms must be accepted. The schema segment must
     * still not make two different databases collide.
     */
    @Test
    void shouldMatchDatasetWhenCatalogTablePathCarriesASchemaSegment() throws Exception {
        LineageDataset ordersInFirstDatabase =
                LineageDataset.of("postgresql://host:5432", "db1.orders");

        Assertions.assertTrue(
                invokeMatchesTablePath(
                        TablePath.of("db1", "public", "orders"), ordersInFirstDatabase));
        Assertions.assertTrue(
                invokeMatchesTablePath(
                        TablePath.of("db1", "reporting", "orders"), ordersInFirstDatabase));
        Assertions.assertFalse(
                invokeMatchesTablePath(
                        TablePath.of("db2", "public", "orders"), ordersInFirstDatabase));
        Assertions.assertFalse(
                invokeMatchesTablePath(
                        TablePath.of("db1", "public", "customers"), ordersInFirstDatabase));
    }

    private static JobMetrics invokeOutputMetrics(JobMaster jobMaster, LineageEventType eventType)
            throws Exception {
        Method method =
                ZetaLineageReporter.class.getDeclaredMethod(
                        "outputMetrics", JobMaster.class, LineageEventType.class);
        method.setAccessible(true);
        return (JobMetrics) method.invoke(null, jobMaster, eventType);
    }

    private static JobMaster mockCurrentMetricsJobMaster(JobMetrics current) {
        JobMaster jobMaster = mock(JobMaster.class);
        doReturn(current).when(jobMaster).getCurrentJobMetricsForLineage();
        return jobMaster;
    }

    private static JobMaster mockHistoryMetricsJobMaster(JobMetrics history) {
        JobMaster jobMaster = mock(JobMaster.class);
        JobHistoryService historyService = mock(JobHistoryService.class);
        doReturn(historyService).when(jobMaster).getJobHistoryService();
        doReturn(1L).when(jobMaster).getJobId();
        doReturn(history).when(historyService).getJobMetrics(1L);
        return jobMaster;
    }

    private static boolean invokeMatchesTablePath(TablePath tablePath, LineageDataset dataset)
            throws Exception {
        Method method =
                ZetaLineageReporter.class.getDeclaredMethod(
                        "matchesTablePath", TablePath.class, LineageDataset.class);
        method.setAccessible(true);
        return (boolean) method.invoke(null, tablePath, dataset);
    }

    private static boolean invokeShouldReportHeartbeat(
            JobMaster jobMaster, int pipelineId, long minimumIntervalMs) throws Exception {
        Method method =
                ZetaLineageReporter.class.getDeclaredMethod(
                        "shouldReportHeartbeat", JobMaster.class, int.class, long.class);
        method.setAccessible(true);
        return (boolean) method.invoke(null, jobMaster, pipelineId, minimumIntervalMs);
    }

    private static JobMetrics metrics(String value) {
        Map<String, List<Measurement>> values = new HashMap<>();
        values.put(
                "marker",
                Collections.singletonList(
                        Measurement.of("marker", value, 1L, Collections.emptyMap())));
        return JobMetrics.of(values);
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = JobMaster.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}
