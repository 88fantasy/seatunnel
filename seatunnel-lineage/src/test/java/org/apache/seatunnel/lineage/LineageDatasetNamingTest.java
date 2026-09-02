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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LineageDatasetNamingTest {

    @Test
    void mapsConnectorOptionsAndKeepsDorisQueryPort() {
        Map<String, Object> options = new HashMap<>();
        options.put("plugin_name", "Doris");
        options.put("fenodes", "fe.example:8030");
        options.put("query-port", 9030);
        options.put("database", "warehouse");
        options.put("table", "orders");

        List<LineageDataset> datasets = LineageDatasetFactory.fromConnectorOptions(options);

        assertEquals("mysql://fe.example:9030", datasets.get(0).namespace());
        assertEquals("warehouse.orders", datasets.get(0).name());
    }

    @Test
    void usesExplicitPaimonCatalogAndTableList() {
        Map<String, Object> first = new HashMap<>();
        first.put("database", "db");
        first.put("table", "one");
        Map<String, Object> options = new HashMap<>();
        options.put("plugin_name", "Paimon");
        options.put("catalog_name", "analytics");
        options.put("table_list", Arrays.asList(first));

        assertEquals(
                "paimon://analytics/db",
                LineageDatasetFactory.fromConnectorOptions(options).get(0).namespace());
    }

    @Test
    void mapsJdbcSourceTablePath() {
        Map<String, Object> options = new HashMap<>();
        options.put("plugin_name", "Jdbc");
        options.put("url", "jdbc:mysql://db.example:3306/connection_default");
        options.put("table_path", "table_database.orders");

        List<LineageDataset> datasets = LineageDatasetFactory.fromConnectorOptions(options);

        assertEquals("mysql://db.example:3306", datasets.get(0).namespace());
        assertEquals("table_database.orders", datasets.get(0).name());
        assertEquals("table_database.orders", datasets.get(0).tablePath());
        assertEquals(
                "table_database.orders",
                datasets.get(0)
                        .withOutputStatistics(new LineageOutputStatistics(1L, 2L, "attempted"))
                        .tablePath());
    }

    @Test
    void skipsDefaultTablePath() {
        Map<String, Object> options = new HashMap<>();
        options.put("plugin_name", "Jdbc");
        options.put("url", "jdbc:mysql://db.example:3306/warehouse");
        options.put("table_path", "default.default.default");

        assertEquals(0, LineageDatasetFactory.fromConnectorOptions(options).size());
    }

    @Test
    void requiresExplicitPaimonCatalog() {
        Map<String, Object> options = new HashMap<>();
        options.put("plugin_name", "Paimon");
        options.put("database", "analytics");
        options.put("table", "orders");

        assertEquals(0, LineageDatasetFactory.fromConnectorOptions(options).size());
    }

    @Test
    void matchesPaimonTablesByTheirCompleteTablePath() {
        Map<String, Object> first = new HashMap<>();
        first.put("database", "db_one");
        first.put("table", "orders");
        Map<String, Object> second = new HashMap<>();
        second.put("database", "db_two");
        second.put("table", "orders");
        Map<String, Object> options = new HashMap<>();
        options.put("plugin_name", "Paimon");
        options.put("catalog_name", "analytics");
        options.put("table_list", Arrays.asList(first, second));

        List<LineageDataset> datasets = LineageDatasetFactory.fromConnectorOptions(options);

        assertEquals(2, datasets.size());
        assertTrue(LineageDatasetNaming.matchesTablePath(datasets.get(0), "db_one.orders"));
        assertFalse(LineageDatasetNaming.matchesTablePath(datasets.get(0), "db_two.orders"));
        assertTrue(LineageDatasetNaming.matchesTablePath(datasets.get(1), "db_two.orders"));
    }

    @Test
    void excludesDefaultTablePathFromEveryConnectorMapping() {
        Map<String, Object> options = new HashMap<>();
        options.put("plugin_name", "Doris");
        options.put("fenodes", "fe.example:8030");
        options.put("database", "warehouse");
        options.put("table", "orders");
        options.put("table_path", "default.default.default");

        assertTrue(LineageDatasetNaming.isDefaultTablePath("default.default.default"));
        assertEquals(0, LineageDatasetFactory.fromConnectorOptions(options).size());
    }

    @Test
    void ignoresMalformedDorisQueryPort() {
        Map<String, Object> options = new HashMap<>();
        options.put("plugin_name", "Doris");
        options.put("fenodes", "fe.example:8030");
        options.put("query-port", "not-a-port");
        options.put("database", "warehouse");
        options.put("table", "orders");

        assertEquals(0, LineageDatasetFactory.fromConnectorOptions(options).size());
    }
}
