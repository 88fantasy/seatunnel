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

import java.util.Optional;

/** Canonical namespace/name mappings for supported table connectors. */
public final class LineageDatasetNaming {
    private static final String DEFAULT_TABLE_PATH = "default.default.default";

    private LineageDatasetNaming() {}

    /**
     * Creates the Paimon dataset identity {@code paimon://catalog/database} and {@code table}.
     *
     * <p>The catalog is required because it is part of the dataset identity.
     */
    public static Optional<LineageDataset> paimon(
            String catalogName, String database, String table) {
        if (isBlank(catalogName) || isBlank(database) || isBlank(table)) {
            return Optional.empty();
        }
        return table(database, table)
                .map(value -> LineageDataset.of("paimon://" + catalogName + "/" + database, value));
    }

    /** Creates the Doris dataset identity using its query port rather than its HTTP port. */
    public static Optional<LineageDataset> doris(
            String feHost, int queryPort, String database, String table) {
        if (isBlank(feHost) || queryPort <= 0) {
            return Optional.empty();
        }
        return table(database, table)
                .map(
                        value ->
                                LineageDataset.of(
                                        "mysql://" + feHost + ":" + queryPort,
                                        database + "." + value));
    }

    /** Creates a JDBC dataset identity from the URL scheme, host, port, database, and table. */
    public static Optional<LineageDataset> jdbc(
            String scheme, String host, int port, String database, String table) {
        if (isBlank(scheme) || isBlank(host) || port <= 0) {
            return Optional.empty();
        }
        return table(database, table)
                .map(
                        value ->
                                LineageDataset.of(
                                        scheme + "://" + host + ":" + port,
                                        database + "." + value));
    }

    /** Returns whether the three-part table path is the placeholder default path. */
    public static boolean isDefaultTablePath(String database, String schema, String table) {
        String path =
                String.valueOf(database)
                        + "."
                        + String.valueOf(schema)
                        + "."
                        + String.valueOf(table);
        return DEFAULT_TABLE_PATH.equals(path);
    }

    /** Returns whether a complete table path is the placeholder default path. */
    public static boolean isDefaultTablePath(String fullName) {
        return fullName != null && DEFAULT_TABLE_PATH.equals(fullName.trim());
    }

    /**
     * Returns whether a dataset was derived from the supplied complete table path.
     *
     * <p>This comparison is deliberately exact. Suffix matching would conflate same-named tables in
     * different databases in a multi-table job.
     */
    public static boolean matchesTablePath(LineageDataset dataset, String fullName) {
        if (dataset == null || fullName == null || fullName.trim().isEmpty()) {
            return false;
        }
        String normalizedPath = fullName.trim();
        if (isDefaultTablePath(normalizedPath)) {
            return false;
        }
        if (dataset.tablePath() != null) {
            return dataset.tablePath().equals(normalizedPath);
        }
        if (dataset.namespace().startsWith("paimon://")) {
            int databaseStart = dataset.namespace().lastIndexOf('/') + 1;
            if (databaseStart <= 0 || databaseStart >= dataset.namespace().length()) {
                return false;
            }
            return (dataset.namespace().substring(databaseStart) + "." + dataset.name())
                    .equals(normalizedPath);
        }
        return dataset.name().equals(normalizedPath);
    }

    private static Optional<String> table(String database, String table) {
        if (isBlank(database) || isBlank(table)) {
            return Optional.empty();
        }
        return Optional.of(table);
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
