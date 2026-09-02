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

import java.io.Serializable;
import java.util.Objects;

/** A dataset identity, optional source table path, and optional output statistics. */
public final class LineageDataset implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String namespace;
    private final String name;
    private final String tablePath;
    private final LineageOutputStatistics outputStatistics;

    private LineageDataset(
            String namespace,
            String name,
            String tablePath,
            LineageOutputStatistics outputStatistics) {
        this.namespace = requireText(namespace, "namespace");
        this.name = requireText(name, "name");
        this.tablePath = blankToNull(tablePath);
        this.outputStatistics = outputStatistics;
    }

    /** Creates a dataset identity without a source-specific table path. */
    public static LineageDataset of(String namespace, String name) {
        return new LineageDataset(namespace, name, null, null);
    }

    /** Creates a dataset identity and records the exact source table path when one is available. */
    public static LineageDataset of(String namespace, String name, String tablePath) {
        return new LineageDataset(namespace, name, tablePath, null);
    }

    /** Returns a copy with output statistics attached. */
    public LineageDataset withOutputStatistics(LineageOutputStatistics statistics) {
        return new LineageDataset(namespace, name, tablePath, statistics);
    }

    /** Returns the dataset namespace. */
    public String namespace() {
        return namespace;
    }

    /** Returns the canonical dataset name. */
    public String name() {
        return name;
    }

    /** Returns the exact source table path used to derive this dataset, when available. */
    public String tablePath() {
        return tablePath;
    }

    /** Returns the output statistics, or {@code null} when no statistics were collected. */
    public LineageOutputStatistics outputStatistics() {
        return outputStatistics;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof LineageDataset)) {
            return false;
        }
        LineageDataset that = (LineageDataset) other;
        return namespace.equals(that.namespace) && name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(namespace, name);
    }

    private static String requireText(String value, String field) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value;
    }

    private static String blankToNull(String value) {
        return value == null || value.trim().isEmpty() ? null : value.trim();
    }
}
