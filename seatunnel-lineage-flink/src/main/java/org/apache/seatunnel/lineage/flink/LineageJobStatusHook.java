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
            if ("onFinished".equals(methodName)) {
                // An attached client emits the successful terminal event with the output
                // statistics read from the job accumulators, which are not reachable from here.
                // Both events describe the same run, and the later arrival wins on the receiver,
                // so exactly one of the two may send it.
                if (!clientReportsCompletion) {
                    emit(LineageEventType.COMPLETE, jobId(args));
                }
            } else if ("onFailed".equals(methodName)) {
                emit(LineageEventType.FAIL, jobId(args));
            } else if ("onCanceled".equals(methodName)) {
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
