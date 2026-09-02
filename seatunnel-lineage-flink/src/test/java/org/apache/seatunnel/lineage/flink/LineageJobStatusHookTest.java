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
import org.apache.seatunnel.lineage.LineageDataset;
import org.apache.seatunnel.lineage.LineageEvent;
import org.apache.seatunnel.lineage.LineageEventType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

class LineageJobStatusHookTest {

    @BeforeEach
    void resetBackend() {
        RecordingLineageBackend.reset();
    }

    private static LineageConfig config(String token) {
        Map<String, Object> options = new LinkedHashMap<>();
        options.put(LineageConfig.ENABLED, true);
        options.put(LineageConfig.TRANSPORT, RecordingLineageBackend.NAME);
        options.put(LineageConfig.URL, "http://127.0.0.1:1");
        LineageConfig resolved =
                LineageConfig.resolve(
                        options,
                        Collections.<String, Object>emptyMap(),
                        Collections.<String, Object>emptyMap());
        return resolved.withAuthToken(token);
    }

    private static LineageEvent event() {
        return LineageEvent.builder()
                .runId(UUID.randomUUID())
                .eventTime(ZonedDateTime.now(ZoneOffset.UTC))
                .eventType(LineageEventType.START)
                .jobNamespace("seatunnel")
                .jobName("hook-test")
                .producer("https://seatunnel.apache.org/test")
                .runFacet("seatunnel_properties")
                .outputs(Collections.singletonList(LineageDataset.of("mysql://db", "sales.orders")))
                .build();
    }

    @SuppressWarnings("unused")
    private static void noop() {}

    @Test
    void emitsTheTerminalEventTypeThatMatchesTheCallback() throws Throwable {
        InvocationHandler hook =
                new LineageJobStatusHook(config(null), Collections.singletonList(event()));
        Method noop = LineageJobStatusHookTest.class.getDeclaredMethod("noop");

        hook.invoke(new Object(), noop, new Object[] {"job-1"});
        Assertions.assertTrue(
                RecordingLineageBackend.EVENTS.isEmpty(), "unknown callbacks emit nothing");

        invoke(hook, "onFinished", "job-1");
        invoke(hook, "onFailed", "job-2");
        invoke(hook, "onCanceled", "job-3");

        Assertions.assertEquals(3, RecordingLineageBackend.EVENTS.size());
        Assertions.assertEquals(
                LineageEventType.COMPLETE, RecordingLineageBackend.EVENTS.get(0).eventType());
        Assertions.assertEquals(
                LineageEventType.FAIL, RecordingLineageBackend.EVENTS.get(1).eventType());
        Assertions.assertEquals(
                LineageEventType.ABORT, RecordingLineageBackend.EVENTS.get(2).eventType());
        Assertions.assertEquals(
                "job-2",
                RecordingLineageBackend.EVENTS
                        .get(1)
                        .runProperties()
                        .get(LineageJobStatusHook.FLINK_JOB_ID_PROPERTY));
    }

    /**
     * The JobGraph carrying this hook is written to the BlobServer and to HA storage, so the
     * instance must never hold a receiver credential. The JobManager resolves the token again from
     * its own configuration and environment.
     */
    @Test
    void survivesTheJobGraphRoundTripWithoutCarryingTheToken() throws Throwable {
        LineageJobStatusHook hook =
                new LineageJobStatusHook(
                        config("secret-token").withAuthToken(null),
                        Collections.singletonList(event()));

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(hook);
        }
        Assertions.assertFalse(
                new String(bytes.toByteArray(), "ISO-8859-1").contains("secret-token"),
                "a serialized hook must not carry the receiver token");

        Object restored;
        try (ObjectInputStream in =
                new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            restored = in.readObject();
        }

        invoke((InvocationHandler) restored, "onFinished", "job-9");
        Assertions.assertEquals(1, RecordingLineageBackend.EVENTS.size());
        Assertions.assertEquals(
                LineageEventType.COMPLETE, RecordingLineageBackend.EVENTS.get(0).eventType());
        Assertions.assertNull(RecordingLineageBackend.CONFIGS.get(0).authToken());
    }

    @Test
    void keepsTheJobRunningWhenTheBackendCannotBeResolved() throws Throwable {
        Map<String, Object> options = new LinkedHashMap<>();
        options.put(LineageConfig.ENABLED, true);
        options.put(LineageConfig.TRANSPORT, "no-such-transport");
        options.put(LineageConfig.URL, "http://127.0.0.1:1");
        LineageConfig broken =
                LineageConfig.resolve(
                        options,
                        Collections.<String, Object>emptyMap(),
                        Collections.<String, Object>emptyMap());

        InvocationHandler hook =
                new LineageJobStatusHook(broken, Collections.singletonList(event()));

        Assertions.assertDoesNotThrow(() -> invoke(hook, "onFinished", "job-1"));
        Assertions.assertTrue(RecordingLineageBackend.EVENTS.isEmpty());
    }

    @Test
    void answersObjectMethodsWithoutEmitting() throws Throwable {
        InvocationHandler hook =
                new LineageJobStatusHook(config(null), Collections.singletonList(event()));
        Object proxy = new Object();

        Assertions.assertEquals(
                "org.apache.flink.core.execution.JobStatusHook",
                hook.invoke(proxy, Object.class.getMethod("toString"), null));
        Assertions.assertEquals(
                System.identityHashCode(proxy),
                hook.invoke(proxy, Object.class.getMethod("hashCode"), null));
        Assertions.assertEquals(
                Boolean.TRUE,
                hook.invoke(
                        proxy,
                        Object.class.getMethod("equals", Object.class),
                        new Object[] {proxy}));
        Assertions.assertEquals(
                Boolean.FALSE,
                hook.invoke(
                        proxy,
                        Object.class.getMethod("equals", Object.class),
                        new Object[] {new Object()}));
        Assertions.assertTrue(RecordingLineageBackend.EVENTS.isEmpty());
    }

    /** Calls a JobStatusHook callback the way the proxy does, by name. */
    private static void invoke(InvocationHandler hook, String callback, String jobId)
            throws Throwable {
        Method method =
                "onFailed".equals(callback)
                        ? StubHook.class.getMethod(callback, Object.class, Throwable.class)
                        : StubHook.class.getMethod(callback, Object.class);
        Object[] args =
                "onFailed".equals(callback)
                        ? new Object[] {jobId, new IllegalStateException("boom")}
                        : new Object[] {jobId};
        hook.invoke(new Object(), method, args);
    }

    /** Mirrors the void callbacks of {@code org.apache.flink.core.execution.JobStatusHook}. */
    private interface StubHook {
        void onCreated(Object jobId);

        void onFinished(Object jobId);

        void onFailed(Object jobId, Throwable failure);

        void onCanceled(Object jobId);
    }
}
