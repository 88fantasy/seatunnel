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
import org.apache.seatunnel.lineage.LineageEvent;
import org.apache.seatunnel.lineage.LineageEventType;
import org.apache.seatunnel.lineage.LineageOutputStatistics;
import org.apache.seatunnel.lineage.LineageReporter;
import org.apache.seatunnel.lineage.LineageReporterFactory;
import org.apache.seatunnel.lineage.LineageRunIds;
import org.apache.seatunnel.lineage.LineageRuntime;
import org.apache.seatunnel.lineage.flink.FlinkClusterOptions;
import org.apache.seatunnel.lineage.flink.LineageJobStatusHook;
import org.apache.seatunnel.lineage.flink.ProxyReturnValues;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Flink 1.20 lineage integration kept binary-compatible with the 1.15 common starter. */
public final class FlinkLineageSupport {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkLineageSupport.class);
    private static final String JOB_STATUS_HOOK = "org.apache.flink.core.execution.JobStatusHook";

    /** The hook implementation the JobManager must be able to load; see {@link #register}. */
    private static final String HOOK_HANDLER_CLASS = LineageJobStatusHook.class.getName();

    /** The artifact that carries {@link LineageJobStatusHook} into a Flink cluster's lib/. */
    private static final String HOOK_ARTIFACT = "seatunnel-lineage-flink-<version>-shaded.jar";

    private FlinkLineageSupport() {}

    /**
     * Returns whether the running Flink version exposes the status-hook API used by lineage.
     *
     * <p>The capability check keeps the common starter compatible with Flink 1.13 and 1.15, where
     * the lineage integration must remain inactive.
     */
    public static boolean isSupported() {
        return findJobStatusHookMethod() != null;
    }

    /** Resolves lineage configuration using the Flink cluster configuration and process env. */
    public static LineageConfig resolveConfig(Map<String, ?> jobOptions) {
        LineageConfig.rejectJobAuthToken(jobOptions);
        if (Boolean.FALSE.equals(enabledValue(jobOptions))) {
            return LineageConfig.defaults();
        }
        return resolveConfig(jobOptions, FlinkClusterOptions.load(), System.getenv());
    }

    /**
     * Resolves lineage configuration without parsing inactive options.
     *
     * <p>The enabled option is checked first with the same job, process-environment, and cluster
     * precedence as the contract. Invalid non-enabled options therefore do not affect a disabled
     * job, while a job-level auth token is still rejected.
     */
    public static LineageConfig resolveConfig(
            Map<String, ?> jobOptions, Map<String, ?> clusterOptions, Map<String, ?> environment) {
        LineageConfig.rejectJobAuthToken(jobOptions);
        if (!isLineageEnabled(jobOptions, clusterOptions, environment)) {
            return LineageConfig.defaults();
        }
        return LineageConfig.resolve(jobOptions, clusterOptions, environment);
    }

    /**
     * Resolves only the lineage enabled flag using job, process-environment, and cluster
     * precedence.
     */
    public static boolean isLineageEnabled(
            Map<String, ?> jobOptions, Map<String, ?> clusterOptions, Map<String, ?> environment) {
        Boolean value = enabledValue(jobOptions);
        if (value != null) {
            return value;
        }
        value = enabledEnvironmentValue(environment);
        if (value != null) {
            return value;
        }
        value = enabledValue(clusterOptions);
        return value != null && value;
    }

    /**
     * Registers the 1.20-only status hook and the attached-mode statistics listener. A null result
     * means that the running Flink version does not expose the hook API.
     */
    public static Registration register(
            StreamExecutionEnvironment environment,
            LineageConfig config,
            JobContext jobContext,
            List<LineageDataset> inputs,
            List<LineageDataset> outputs,
            String jobName) {
        try {
            if (config == null || !config.enabled()) {
                return null;
            }
            if (outputs == null || outputs.isEmpty()) {
                // Reporting is enabled, so producing nothing is a result the operator needs to
                // see; otherwise a job whose sinks resolved to no dataset looks identical to a
                // working one that simply has no lineage yet.
                LOGGER.info(
                        "No lineage events built for job {}: none of its sinks resolved to a"
                                + " supported dataset",
                        jobName);
                return null;
            }
            Method registerHook = findJobStatusHookMethod();
            if (registerHook == null) {
                LOGGER.warn(
                        "Lineage reporting is enabled but the running Flink version does not"
                                + " expose the job status hook API, which requires Flink 1.16 or"
                                + " later. No lineage events will be reported for job {}.",
                        jobName);
                return null;
            }
            List<LineageEvent> events = createEvents(config, jobContext, inputs, outputs, jobName);
            try {
                LineageReporter reporter = LineageReporterFactory.create(config);
                for (LineageEvent event : events) {
                    reporter.start(event);
                }
            } catch (Throwable error) {
                LOGGER.warn("Failed to emit Flink lineage start event", error);
            }

            // Explicitly non-clearing: the no-argument overload clears the environment's
            // transformations, so any later failure here would leave the caller's fallback
            // execute(jobName) with an empty topology instead of a job that simply has no lineage.
            StreamGraph streamGraph = environment.getStreamGraph(false);
            streamGraph.setJobName(jobName);
            boolean clientReportsTerminalEvent = isAttachedSubmission(environment);
            Class<?> hookType = registerHook.getParameterTypes()[0];
            Object hook =
                    Proxy.newProxyInstance(
                            hookType.getClassLoader(),
                            new Class<?>[] {hookType},
                            new LineageJobStatusHook(
                                    config.withAuthToken(null),
                                    events,
                                    clientReportsTerminalEvent));
            registerHook.invoke(streamGraph, hook);
            // Stated before submission because the JobManager-side failure that follows a missing
            // deployment is a bare ClassNotFoundException that never mentions lineage.
            LOGGER.info(
                    "Registered the lineage job status hook; the JobManager must be able to load {}"
                            + " from {} in $FLINK_HOME/lib, otherwise job submission fails",
                    HOOK_HANDLER_CLASS,
                    HOOK_ARTIFACT);
            try {
                environment.registerJobListener(
                        (JobListener)
                                Proxy.newProxyInstance(
                                        JobListener.class.getClassLoader(),
                                        new Class<?>[] {JobListener.class},
                                        new JobListenerInvocationHandler(
                                                config.withAuthToken(null), events)));
            } catch (Throwable error) {
                LOGGER.warn("Failed to register Flink lineage result listener", error);
            }
            return new Registration(streamGraph);
        } catch (Throwable error) {
            LOGGER.warn("Failed to register Flink lineage support: {}", error.toString(), error);
            return null;
        }
    }

    /**
     * Returns whether the submission is attached, and therefore whether the client will report the
     * terminal event itself.
     *
     * <p>This reads the same option Flink itself branches on in {@code
     * StreamExecutionEnvironment.execute(StreamGraph)}, so the answer cannot disagree with which
     * callback actually fires. It decides ownership of the terminal event: both the JobManager hook
     * and the client listener observe the same run, but only the client can read the accumulators
     * that carry the output statistics, so in attached mode the hook must stay silent rather than
     * race a second COMPLETE that would overwrite them.
     *
     * <p>An unreadable configuration answers {@code false}, which keeps the hook reporting: a
     * duplicated terminal event is recoverable, a missing one is not.
     */
    private static boolean isAttachedSubmission(StreamExecutionEnvironment environment) {
        try {
            return Boolean.TRUE.equals(
                    environment.getConfiguration().get(DeploymentOptions.ATTACHED));
        } catch (Throwable error) {
            LOGGER.warn("Unable to determine whether the Flink submission is attached", error);
            return false;
        }
    }

    private static Method findJobStatusHookMethod() {
        try {
            Class<?> hookType = Class.forName(JOB_STATUS_HOOK);
            for (Method method : StreamGraph.class.getMethods()) {
                if ("registerJobStatusHook".equals(method.getName())
                        && method.getParameterCount() == 1
                        && method.getParameterTypes()[0].equals(hookType)) {
                    return method;
                }
            }
        } catch (Throwable error) {
            LOGGER.debug("Flink status-hook API is unavailable", error);
        }
        return null;
    }

    /**
     * Explains a job submission that failed because the status hook is missing on the JobManager.
     *
     * <p>The hook is a structural field of the JobGraph, not user code, so the JobManager
     * deserializes it with the system class loader before any user class loader exists. Shipping
     * the class in the submitted job jar is therefore not enough, and Flink reports only a bare
     * {@code ClassNotFoundException} from {@code JobSubmitHandler.loadJobGraph}, which does not say
     * that lineage caused it or how to fix it.
     *
     * <p>Failing the submission is intended: a job that silently loses its lineage is worse than
     * one that refuses to start. This only makes the reason actionable.
     *
     * <p>The failure is recognised from the message rather than from the exception type, because
     * the deployment this exists for is a session cluster submitted over REST: there the
     * JobManager-side failure never crosses the wire as an exception object. The client sees a
     * {@code RestClientException} whose message carries the server-side stack trace as text, so a
     * type check alone would never match. The message must report the hook class as the one that
     * could not be loaded, so neither an unrelated failure that mentions the class nor an unrelated
     * missing class in the same trace is misreported as a missing deployment.
     *
     * @param failure the exception thrown by job submission
     * @return an explanatory message, or {@code null} when the failure is unrelated to lineage
     */
    public static String describeMissingHookClass(Throwable failure) {
        for (Throwable current = failure;
                current != null && current != current.getCause();
                current = current.getCause()) {
            String message = current.getMessage();
            if (message == null || !message.contains(HOOK_HANDLER_CLASS)) {
                continue;
            }
            if (!reportsUnloadableHookClass(current, message)) {
                continue;
            }
            return "Execute Flink job error: lineage reporting is enabled, but the JobManager"
                    + " cannot load "
                    + HOOK_HANDLER_CLASS
                    + ". The job status hook is part of the JobGraph and is deserialized by the"
                    + " JobManager before the user class loader exists, so shipping it in the"
                    + " submitted jar is not enough. Install "
                    + HOOK_ARTIFACT
                    + " in $FLINK_HOME/lib on the JobManager and restart it, or set"
                    + " openlineage_enabled=false to submit without lineage.";
        }
        return null;
    }

    /**
     * Returns whether a link in the cause chain reports that the hook class could not be loaded,
     * either as the exception type or, for a failure relayed as text across the REST boundary, in
     * its message.
     *
     * <p>A relayed message carries a whole server-side stack trace, which can name several failures
     * at once. The class-loading failure and the hook class must therefore appear as one {@code
     * Throwable.toString()} pair rather than merely somewhere in the same text; otherwise a
     * submission that failed over an unrelated missing class would be reported as a missing lineage
     * deployment, and the caller replaces the top-level message with that explanation.
     */
    private static boolean reportsUnloadableHookClass(Throwable current, String message) {
        if (current instanceof ClassNotFoundException || current instanceof NoClassDefFoundError) {
            return true;
        }
        String binaryName = HOOK_HANDLER_CLASS;
        // NoClassDefFoundError names the class in internal form.
        String internalName = HOOK_HANDLER_CLASS.replace('.', '/');
        return message.contains(ClassNotFoundException.class.getName() + ": " + binaryName)
                || message.contains(NoClassDefFoundError.class.getName() + ": " + binaryName)
                || message.contains(NoClassDefFoundError.class.getName() + ": " + internalName);
    }

    /** Returns whether a completed result is attached and safe to use for output statistics. */
    public static boolean isAttachedJobExecutionResult(
            JobExecutionResult result, Throwable executionError) {
        try {
            return executionError == null && result != null && result.isJobExecutionResult();
        } catch (Throwable error) {
            LOGGER.warn("Unable to determine whether Flink execution was attached", error);
            return false;
        }
    }

    private static List<LineageEvent> createEvents(
            LineageConfig config,
            JobContext jobContext,
            List<LineageDataset> inputs,
            List<LineageDataset> outputs,
            String jobName) {
        List<LineageEvent> events = new ArrayList<>();
        for (LineageDataset output : outputs) {
            Map<String, Object> properties = new LinkedHashMap<>(config.runProperties());
            properties.put("engine", "flink");
            events.add(
                    LineageEvent.builder()
                            .runId(
                                    LineageRunIds.forJob(
                                            jobContext.getJobId(),
                                            output.namespace(),
                                            output.name(),
                                            null))
                            .eventTime(ZonedDateTime.now(ZoneOffset.UTC))
                            .eventType(LineageEventType.START)
                            .jobNamespace(config.namespace())
                            .jobName(jobName)
                            .producer(config.producer())
                            .runFacet(config.runFacet())
                            .runProperties(properties)
                            .inputs(inputs)
                            .outputs(Collections.singletonList(output))
                            .build());
        }
        return events;
    }

    private static final class JobListenerInvocationHandler implements InvocationHandler {
        private final LineageConfig config;
        private final List<LineageEvent> events;

        private JobListenerInvocationHandler(LineageConfig config, List<LineageEvent> events) {
            this.config = config;
            this.events = events;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            try {
                if (!"onJobExecuted".equals(method.getName())
                        || args == null
                        || args.length < 2
                        || !(args[0] instanceof JobExecutionResult)
                        || !isAttachedJobExecutionResult(
                                (JobExecutionResult) args[0], (Throwable) args[1])) {
                    return ProxyReturnValues.defaultFor(method);
                }
                JobExecutionResult result = (JobExecutionResult) args[0];
                Map<String, Object> accumulators = result.getAllAccumulatorResults();
                Long rowCount = number(accumulators.get(MetricNames.SINK_WRITE_COUNT));
                Long size = number(accumulators.get(MetricNames.SINK_WRITE_BYTES));
                LineageOutputStatistics statistics =
                        positive(rowCount) || positive(size)
                                ? new LineageOutputStatistics(rowCount, size, "attempted")
                                : null;
                LineageConfig runtimeConfig =
                        config.withAuthToken(
                                LineageConfig.resolveToken(
                                        FlinkClusterOptions.load(), System.getenv()));
                String flinkJobId = result.getJobID() == null ? null : result.getJobID().toString();
                for (LineageEvent event : events) {
                    // Emitted even without statistics: an attached submission silences the
                    // JobManager hook, so skipping here would leave the run with no terminal event
                    // at all rather than one that is merely missing its row counts.
                    LineageEvent completed =
                            statistics == null ? event : event.withOutputStatistics(statistics);
                    LineageRuntime.emit(
                            runtimeConfig,
                            completed
                                    .withEventType(LineageEventType.COMPLETE)
                                    .withRunProperty(
                                            LineageJobStatusHook.FLINK_JOB_ID_PROPERTY,
                                            flinkJobId));
                }
            } catch (Throwable error) {
                LOGGER.warn("Failed to emit Flink output statistics", error);
            }
            return ProxyReturnValues.defaultFor(method);
        }

        private static Long number(Object value) {
            return value instanceof Number ? ((Number) value).longValue() : null;
        }

        private static boolean positive(Long value) {
            return value != null && value > 0;
        }
    }

    public static final class Registration {
        private final StreamGraph streamGraph;

        private Registration(StreamGraph streamGraph) {
            this.streamGraph = streamGraph;
        }

        public StreamGraph getStreamGraph() {
            return streamGraph;
        }
    }

    private static Boolean enabledValue(Map<?, ?> values) {
        OptionValue option = findOption(values, LineageConfig.ENABLED);
        if (!option.present) {
            return null;
        }
        return option.value != null && Boolean.parseBoolean(String.valueOf(option.value));
    }

    private static Boolean enabledEnvironmentValue(Map<?, ?> values) {
        if (values == null || values.isEmpty()) {
            return null;
        }
        String key = LineageConfig.ENABLED.toUpperCase(Locale.ROOT);
        if (!values.containsKey(key)) {
            return null;
        }
        Object value = values.get(key);
        return value != null && Boolean.parseBoolean(String.valueOf(value));
    }

    private static OptionValue findOption(Map<?, ?> values, String key) {
        if (values == null || values.isEmpty()) {
            return OptionValue.absent();
        }
        if (values.containsKey(key)) {
            return OptionValue.of(values.get(key));
        }
        String dotted = key.replace('_', '.');
        if (values.containsKey(dotted)) {
            return OptionValue.of(values.get(dotted));
        }
        Object nested = values.get("openlineage");
        if (nested instanceof Map) {
            Map<?, ?> nestedOptions = (Map<?, ?>) nested;
            String suffix = key.substring("openlineage_".length());
            if (nestedOptions.containsKey(suffix)) {
                return OptionValue.of(nestedOptions.get(suffix));
            }
            if (nestedOptions.containsKey(key)) {
                return OptionValue.of(nestedOptions.get(key));
            }
        }
        return OptionValue.absent();
    }

    private static final class OptionValue {
        private final boolean present;
        private final Object value;

        private OptionValue(boolean present, Object value) {
            this.present = present;
            this.value = value;
        }

        private static OptionValue of(Object value) {
            return new OptionValue(true, value);
        }

        private static OptionValue absent() {
            return new OptionValue(false, null);
        }
    }
}
