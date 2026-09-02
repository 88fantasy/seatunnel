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

package org.apache.seatunnel.lineage;

/** Creates stateless reporters backed by the separately packaged SPI implementation. */
public final class LineageReporterFactory {
    private LineageReporterFactory() {}

    /** Creates a no-op reporter when disabled, or an SPI-backed reporter otherwise. */
    public static LineageReporter create(LineageConfig config) {
        if (!config.enabled()) {
            return new NoopLineageReporter();
        }
        LineageBackend backend = LineageBackendLoader.load(config);
        return new DefaultLineageReporter(config, backend);
    }

    private static final class DefaultLineageReporter implements LineageReporter {
        private final LineageConfig config;
        private final LineageBackend backend;

        private DefaultLineageReporter(LineageConfig config, LineageBackend backend) {
            this.config = config;
            this.backend = backend;
        }

        @Override
        public void start(LineageEvent event) {
            emit(event.withEventType(LineageEventType.START));
        }

        @Override
        public void heartbeat(LineageEvent event) {
            emit(event.withEventType(LineageEventType.RUNNING));
        }

        @Override
        public void complete(LineageEvent event) {
            emit(event.withEventType(LineageEventType.COMPLETE));
        }

        private void emit(LineageEvent event) {
            try {
                backend.emit(config, event);
            } catch (Exception e) {
                throw new LineageReportingException(e);
            }
        }
    }

    private static final class NoopLineageReporter implements LineageReporter {
        @Override
        public void start(LineageEvent event) {}

        @Override
        public void heartbeat(LineageEvent event) {}

        @Override
        public void complete(LineageEvent event) {}
    }
}
