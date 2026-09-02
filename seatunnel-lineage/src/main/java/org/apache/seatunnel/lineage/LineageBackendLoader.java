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

import java.util.ServiceLoader;

/** Loads the backend selected by the immutable lineage configuration. */
public final class LineageBackendLoader {
    private LineageBackendLoader() {}

    /** Loads the backend whose name matches the configured transport. */
    public static LineageBackend load(LineageConfig config) {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        if (loader == null) {
            loader = LineageBackendLoader.class.getClassLoader();
        }
        for (LineageBackend candidate : ServiceLoader.load(LineageBackend.class, loader)) {
            if (config.transport().equalsIgnoreCase(candidate.getName())) {
                return candidate;
            }
        }
        throw new IllegalArgumentException(
                "No LineageBackend is registered for transport " + config.transport());
    }
}
