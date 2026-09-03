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

package org.apache.seatunnel.lineage.flink;

import org.apache.seatunnel.lineage.LineageConfig;
import org.apache.seatunnel.lineage.LineageEvent;
import org.apache.seatunnel.lineage.LineageEventType;
import org.apache.seatunnel.lineage.LineageRuntime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Emits the terminal lineage event for a Flink job from the JobManager.
 *
 * <p>This is the handler behind a {@code org.apache.flink.core.execution.JobStatusHook} proxy. It
 * is a proxy rather than a direct implementation so that the Flink starter, which compiles against
 * Flink 1.15, stays source compatible while targeting the 1.16+ hook API.
 *
 * <p>This class is the reason the lineage artifact has to be installed in the Flink cluster's
 * {@code lib/} directory. The hook is a structural field of the JobGraph, so the JobManager
 * deserializes it with the system class loader before any user class loader exists; the class is
 * not resolvable from the submitted job jar.
 *
 * <p>The instance is serialized into the JobGraph, which is written to the BlobServer and to HA
 * storage. It therefore carries no credential: the caller strips the token, and the token is
 * resolved again on the JobManager from its own cluster configuration and process environment.
 */
public final class LineageJobStatusHook implements InvocationHandler, Serializable {
    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = LoggerFactory.getLogger(LineageJobStatusHook.class);

    private static final String JOB_STATUS_HOOK = "org.apache.flink.core.execution.JobStatusHook";

    /** Run-facet property carrying the Flink job identifier shown in the Flink UI and REST API. */
    public static final String FLINK_JOB_ID_PROPERTY = "flink_job_id";

    private final LineageConfig config;
    private final List<LineageEvent> events;
    private final boolean clientReportsCompletion;

    /**
     * Cancelled by whichever terminal callback fires. Transient because the hook is serialized into
     * the JobGraph on the client, where no heartbeat is running; the JobManager starts its own on
     * {@code onCreated}. Volatile because the callbacks that start and stop it are not guaranteed
     * to run on one thread.
     */
    private transient volatile ScheduledFuture<?> heartbeat;

    /**
     * @param config lineage configuration with the auth token already removed
     * @param events the start events whose runs this hook closes
     * @param clientReportsCompletion whether the submitting client reports the successful terminal
     *     event itself, in which case this hook must not send a second one
     */
    public LineageJobStatusHook(
            LineageConfig config, List<LineageEvent> events, boolean clientReportsCompletion) {
        this.config = config;
        this.events = new ArrayList<>(events);
        this.clientReportsCompletion = clientReportsCompletion;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) {
        try {
            String methodName = method.getName();
            if ("toString".equals(methodName)) {
                return JOB_STATUS_HOOK;
            }
            if ("hashCode".equals(methodName)) {
                return System.identityHashCode(proxy);
            }
            if ("equals".equals(methodName)) {
                return args != null && args.length == 1 && proxy == args[0];
            }
            if ("onCreated".equals(methodName)) {
                startHeartbeat(jobId(args));
            } else if ("onFinished".equals(methodName)) {
                stopHeartbeat();
                // An attached client emits the successful terminal event with the output
                // statistics read from the job accumulators, which are not reachable from here.
                // Both events describe the same run, and the later arrival wins on the receiver,
                // so exactly one of the two may send it.
                if (!clientReportsCompletion) {
                    emit(LineageEventType.COMPLETE, jobId(args));
                }
            } else if ("onFailed".equals(methodName)) {
                stopHeartbeat();
                emit(LineageEventType.FAIL, jobId(args));
            } else if ("onCanceled".equals(methodName)) {
                stopHeartbeat();
                emit(LineageEventType.ABORT, jobId(args));
            }
        } catch (Throwable error) {
            LOGGER.warn("Failed to handle Flink lineage status callback", error);
        }
        return ProxyReturnValues.defaultFor(method);
    }

    /**
     * Extracts the Flink job identifier passed to every {@code JobStatusHook} callback.
     *
     * <p>The run ID is derived before submission, when no Flink job ID exists yet, so this is the
     * only point where the lineage run can be tied back to the job visible in the Flink UI.
     */
    private static String jobId(Object[] args) {
        return args != null && args.length > 0 && args[0] != null ? String.valueOf(args[0]) : null;
    }

    /**
     * Starts the periodic RUNNING event that keeps the receiver from treating a still-running job
     * as abandoned.
     *
     * <p>A receiver decides a run was abandoned from how long it has been silent, so a job that
     * outlives that window without reporting has its lineage dropped from the current graph even
     * though it is healthy. Zeta reports from its checkpoint completion callback, but Flink offers
     * the JobManager no periodic callback to hang this on: {@code JobStatusHook} is purely
     * transitional, and the client's {@code JobListener} is gone once a detached submission
     * returns. A scheduled task is therefore the only place this can live for a detached job, which
     * is how streaming jobs are normally submitted.
     *
     * <p>Batch jobs are not excluded. The interval is coarse enough that a batch job finishes long
     * before the first heartbeat, and one that does run for hours needs the heartbeat for the same
     * reason a streaming job does.
     *
     * <p>The first heartbeat is one full interval away because the start event has just refreshed
     * the run.
     */
    private void startHeartbeat(String flinkJobId) {
        long intervalMs = config.heartbeatMinIntervalMs();
        if (intervalMs <= 0) {
            return;
        }
        try {
            heartbeat =
                    Heartbeats.SCHEDULER.scheduleWithFixedDelay(
                            () -> emit(LineageEventType.RUNNING, flinkJobId),
                            intervalMs,
                            intervalMs,
                            TimeUnit.MILLISECONDS);
        } catch (Throwable error) {
            // A job must not fail because its lineage heartbeat could not be scheduled.
            LOGGER.warn("Failed to schedule Flink lineage heartbeat", error);
        }
    }

    /** Stops the heartbeat before the terminal event, so no RUNNING can follow it. */
    private void stopHeartbeat() {
        ScheduledFuture<?> scheduled = heartbeat;
        if (scheduled != null) {
            scheduled.cancel(false);
            heartbeat = null;
        }
    }

    /**
     * Holds the one scheduler shared by every job on this JobManager.
     *
     * <p>A single daemon thread rather than one per job: heartbeats are hours apart, and {@code
     * scheduleWithFixedDelay} lets a slow send push its own next run back instead of piling up.
     * Daemon so a JobManager shutdown is never held open by it.
     */
    private static final class Heartbeats {
        static final ScheduledExecutorService SCHEDULER =
                Executors.newSingleThreadScheduledExecutor(
                        runnable -> {
                            Thread thread = new Thread(runnable, "seatunnel-lineage-heartbeat");
                            thread.setDaemon(true);
                            return thread;
                        });

        private Heartbeats() {}
    }

    private void emit(LineageEventType eventType, String flinkJobId) {
        try {
            LineageConfig runtimeConfig =
                    config.withAuthToken(
                            LineageConfig.resolveToken(
                                    FlinkClusterOptions.load(), System.getenv()));
            for (LineageEvent event : events) {
                LineageRuntime.emit(
                        runtimeConfig,
                        event.withEventType(eventType)
                                .withRunProperty(FLINK_JOB_ID_PROPERTY, flinkJobId));
            }
        } catch (Throwable error) {
            LOGGER.warn("Failed to emit Flink lineage status event", error);
        }
    }
}
