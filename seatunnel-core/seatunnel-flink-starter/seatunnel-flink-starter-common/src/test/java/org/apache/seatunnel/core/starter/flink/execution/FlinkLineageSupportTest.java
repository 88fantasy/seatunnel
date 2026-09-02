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

package org.apache.seatunnel.core.starter.flink.execution;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.metrics.MetricNames;
import org.apache.seatunnel.lineage.LineageConfig;
import org.apache.seatunnel.lineage.LineageDataset;
import org.apache.seatunnel.lineage.flink.LineageJobStatusHook;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.execution.DetachedJobExecutionResult;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OptionalFailure;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

class FlinkLineageSupportTest {

    @Test
    void shouldNotParseInactiveLineageOptions() {
        Map<String, Object> jobOptions = new LinkedHashMap<>();
        jobOptions.put(LineageConfig.ENABLED, false);
        jobOptions.put(LineageConfig.TIMEOUT_MS, "not-a-number");

        LineageConfig config =
                FlinkLineageSupport.resolveConfig(
                        jobOptions,
                        Collections.<String, Object>emptyMap(),
                        Collections.<String, Object>emptyMap());

        Assertions.assertFalse(config.enabled());
    }

    @Test
    void shouldResolveEnablementWithJobEnvironmentClusterPrecedence() {
        Map<String, Object> clusterOptions = Collections.singletonMap(LineageConfig.ENABLED, true);
        Map<String, Object> environment = Collections.singletonMap("OPENLINEAGE_ENABLED", false);

        LineageConfig environmentConfig =
                FlinkLineageSupport.resolveConfig(
                        Collections.<String, Object>emptyMap(), clusterOptions, environment);
        Assertions.assertFalse(environmentConfig.enabled());

        Map<String, Object> jobOptions = Collections.singletonMap(LineageConfig.ENABLED, true);
        LineageConfig jobConfig =
                FlinkLineageSupport.resolveConfig(jobOptions, clusterOptions, environment);
        Assertions.assertTrue(jobConfig.enabled());

        Map<String, Object> disabledJobOptions =
                Collections.singletonMap(LineageConfig.ENABLED, false);
        Assertions.assertFalse(
                FlinkLineageSupport.isLineageEnabled(
                        disabledJobOptions,
                        clusterOptions,
                        Collections.singletonMap("OPENLINEAGE_ENABLED", true)));
    }

    @Test
    void shouldRejectJobTokenEvenWhenLineageIsDisabled() {
        Map<String, Object> jobOptions = new LinkedHashMap<>();
        jobOptions.put(LineageConfig.AUTH_TOKEN, "job-token");
        jobOptions.put(LineageConfig.ENABLED, false);

        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        FlinkLineageSupport.resolveConfig(
                                jobOptions,
                                Collections.<String, Object>emptyMap(),
                                Collections.<String, Object>emptyMap()));
    }

    @Test
    void shouldDetectUnsupportedFlinkRuntimeWithoutStatusHook() {
        Assertions.assertFalse(FlinkLineageSupport.isSupported());
    }

    @Test
    void shouldReportStatisticsOnlyForAttachedExecutionResult() {
        JobExecutionResult attached =
                new JobExecutionResult(new JobID(), 1L, Collections.emptyMap());
        DetachedJobExecutionResult detached = new DetachedJobExecutionResult(new JobID());

        Assertions.assertTrue(FlinkLineageSupport.isAttachedJobExecutionResult(attached, null));
        Assertions.assertFalse(FlinkLineageSupport.isAttachedJobExecutionResult(detached, null));
        Assertions.assertFalse(
                FlinkLineageSupport.isAttachedJobExecutionResult(
                        attached, new IllegalStateException("job failed")));
        Assertions.assertFalse(FlinkLineageSupport.isAttachedJobExecutionResult(null, null));
    }

    @Test
    void shouldKeepFlinkCallbacksBestEffortWhenLineageBackendFails() throws Exception {
        Assumptions.assumeTrue(FlinkLineageSupport.isSupported());

        Map<String, Object> options = new LinkedHashMap<>();
        options.put(LineageConfig.ENABLED, true);
        options.put(LineageConfig.URL, "http://127.0.0.1:1");
        LineageConfig config =
                FlinkLineageSupport.resolveConfig(
                        options,
                        Collections.<String, Object>emptyMap(),
                        Collections.<String, Object>emptyMap());
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        environment.fromElements(1).print();
        FlinkLineageSupport.Registration registration =
                Assertions.assertDoesNotThrow(
                        () ->
                                FlinkLineageSupport.register(
                                        environment,
                                        config,
                                        new JobContext(),
                                        Collections.<LineageDataset>emptyList(),
                                        Collections.singletonList(
                                                LineageDataset.of(
                                                        "mysql://db", "warehouse.orders")),
                                        "lineage-test"));

        Assertions.assertNotNull(registration);
        Assertions.assertEquals(1, environment.getJobListeners().size());
        JobListener listener = environment.getJobListeners().get(0);
        Map<String, OptionalFailure<Object>> accumulatorResults = new HashMap<>();
        accumulatorResults.put(
                MetricNames.SINK_WRITE_COUNT, OptionalFailure.of((Object) Long.valueOf(1L)));
        JobExecutionResult attached = new JobExecutionResult(new JobID(), 1L, accumulatorResults);
        Assertions.assertDoesNotThrow(() -> listener.onJobExecuted(attached, null));
        Assertions.assertDoesNotThrow(
                () -> listener.onJobExecuted(new DetachedJobExecutionResult(new JobID()), null));

        Method getHooks = registration.getStreamGraph().getClass().getMethod("getJobStatusHooks");
        List<?> hooks = (List<?>) getHooks.invoke(registration.getStreamGraph());
        Assertions.assertEquals(1, hooks.size());
        Class<?> hookType = Class.forName("org.apache.flink.core.execution.JobStatusHook");
        Object hook = hooks.get(0);
        Assertions.assertDoesNotThrow(
                () -> hookType.getMethod("onFinished", JobID.class).invoke(hook, new JobID()));
        Assertions.assertDoesNotThrow(
                () ->
                        hookType.getMethod("onFailed", JobID.class, Throwable.class)
                                .invoke(hook, new JobID(), new IllegalStateException("failed")));
        Assertions.assertDoesNotThrow(
                () -> hookType.getMethod("onCanceled", JobID.class).invoke(hook, new JobID()));
    }

    /**
     * Reproduces the exception chain a real detached submission produces when the hook class is not
     * on the JobManager. Flink surfaces only a bare ClassNotFoundException from
     * JobSubmitHandler.loadJobGraph, which names the class but never mentions lineage or the fix.
     */
    @Test
    void explainsAJobManagerThatCannotLoadTheStatusHook() {
        String hookClass = LineageJobStatusHook.class.getName();
        Exception submissionFailure =
                new RuntimeException(
                        "Failed to submit JobGraph.",
                        new RuntimeException(
                                "Failed to deserialize JobGraph.",
                                new ClassNotFoundException(hookClass)));

        String hint = FlinkLineageSupport.describeMissingHookClass(submissionFailure);

        Assertions.assertNotNull(hint);
        Assertions.assertTrue(hint.contains("$FLINK_HOME/lib"), "must say where to install it");
        Assertions.assertTrue(
                hint.contains("seatunnel-lineage-flink"), "must name the artifact to install");
        Assertions.assertTrue(
                hint.contains("openlineage_enabled=false"),
                "must offer a way to submit without it");
        Assertions.assertTrue(hint.contains(hookClass), "must name the class Flink could not load");
    }

    @Test
    void leavesUnrelatedSubmissionFailuresUntouched() {
        Assertions.assertNull(
                FlinkLineageSupport.describeMissingHookClass(
                        new RuntimeException(
                                "Failed to submit JobGraph.",
                                new ClassNotFoundException("com.example.SomeConnector"))));
        Assertions.assertNull(
                FlinkLineageSupport.describeMissingHookClass(
                        new IllegalStateException("Recovery is suppressed")));
        Assertions.assertNull(FlinkLineageSupport.describeMissingHookClass(null));
    }
}
