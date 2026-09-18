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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LineageDatasetFactoryTest {

    /**
     * A SeaTunnel job normally declares a connector as a named block, for example {@code sink {
     * Paimon { ... } }}. The option map for that block carries no {@code plugin_name} entry, so the
     * caller must pass the plugin name; otherwise the most common job syntax silently produces no
     * lineage at all.
     */
    @Test
    void usesTheSuppliedPluginNameWhenOptionsDoNotCarryOne() {
        Map<String, Object> options = paimonOptions();

        assertTrue(
                LineageDatasetFactory.fromConnectorOptions(options).isEmpty(),
                "options from a named block cannot identify the connector on their own");

        List<LineageDataset> datasets =
                LineageDatasetFactory.fromConnectorOptions("Paimon", options);

        assertEquals(1, datasets.size());
        assertEquals("paimon://paimon_s3/seatunnel_lineage_verify", datasets.get(0).namespace());
        assertEquals("st_verify_paimon", datasets.get(0).name());
    }

    @Test
    void stillReadsThePluginNameFromOptionsWhenPresent() {
        Map<String, Object> options = paimonOptions();
        options.put("plugin_name", "Paimon");

        List<LineageDataset> datasets = LineageDatasetFactory.fromConnectorOptions(options);

        assertEquals(1, datasets.size());
        assertEquals("paimon://paimon_s3/seatunnel_lineage_verify", datasets.get(0).namespace());
    }

    /**
     * Paimon's catalog is part of the dataset identity, so an absent catalog must not be guessed.
     */
    @Test
    void emitsNothingForPaimonWithoutAnExplicitCatalog() {
        Map<String, Object> options = paimonOptions();
        options.remove("catalog_name");

        assertTrue(LineageDatasetFactory.fromConnectorOptions("Paimon", options).isEmpty());
    }

    /**
     * Doris advertises its Stream Load HTTP port in {@code fenodes}, but the lineage namespace must
     * use the MySQL query port, otherwise the table splits into two nodes in the graph.
     */
    @Test
    void usesTheDorisQueryPortRatherThanTheFenodesPort() {
        Map<String, Object> options = new LinkedHashMap<>();
        options.put("fenodes", "192.168.10.131:8030");
        options.put("database", "ds71");
        options.put("table", "orders");

        List<LineageDataset> datasets =
                LineageDatasetFactory.fromConnectorOptions("Doris", options);

        assertEquals(1, datasets.size());
        assertEquals("mysql://192.168.10.131:9030", datasets.get(0).namespace());
        assertEquals("ds71.orders", datasets.get(0).name());
    }

    /**
     * MySQL-CDC names its tables through {@code table-names} as fully qualified {@code db.table}
     * entries and keeps the {@code mysql://} scheme, so it merges with JDBC reads of the same
     * table.
     */
    @Test
    void readsMySqlCdcTableNamesWithUrl() {
        Map<String, Object> options = new LinkedHashMap<>();
        options.put("url", "jdbc:mysql://192.168.10.131:3306");
        options.put("table-names", Arrays.asList("ds71.orders"));

        List<LineageDataset> datasets =
                LineageDatasetFactory.fromConnectorOptions("MySQL-CDC", options);

        assertEquals(1, datasets.size());
        assertEquals("mysql://192.168.10.131:3306", datasets.get(0).namespace());
        assertEquals("ds71.orders", datasets.get(0).name());
    }

    /** {@code base-url} is the fallback key of the CDC {@code url} option. */
    @Test
    void readsMySqlCdcTableNamesWithBaseUrl() {
        Map<String, Object> options = new LinkedHashMap<>();
        options.put("base-url", "jdbc:mysql://192.168.10.131:3306/ds71");
        options.put("table-names", Arrays.asList("ds71.orders"));

        List<LineageDataset> datasets =
                LineageDatasetFactory.fromConnectorOptions("MySQL-CDC", options);

        assertEquals(1, datasets.size());
        assertEquals("mysql://192.168.10.131:3306", datasets.get(0).namespace());
        assertEquals("ds71.orders", datasets.get(0).name());
    }

    /**
     * Every {@code table-names} entry becomes its own dataset; {@code database-names} only filters
     * the binlog subscription and must not requalify the listed tables.
     */
    @Test
    void expandsEveryMySqlCdcTableName() {
        Map<String, Object> options = new LinkedHashMap<>();
        options.put("base-url", "jdbc:mysql://192.168.10.131:3306");
        options.put("database-names", Arrays.asList("ds71"));
        options.put("table-names", Arrays.asList("ds71.orders", "ds72.customers"));

        List<LineageDataset> datasets =
                LineageDatasetFactory.fromConnectorOptions("MySQL-CDC", options);

        assertEquals(2, datasets.size());
        assertEquals("mysql://192.168.10.131:3306", datasets.get(0).namespace());
        assertEquals("ds71.orders", datasets.get(0).name());
        assertEquals("mysql://192.168.10.131:3306", datasets.get(1).namespace());
        assertEquals("ds72.customers", datasets.get(1).name());
    }

    @Test
    void stillReadsJdbcTablePathAndTableList() {
        Map<String, Object> single = new LinkedHashMap<>();
        single.put("url", "jdbc:mysql://192.168.10.131:3306/ds71");
        single.put("table_path", "ds71.orders");

        List<LineageDataset> datasets = LineageDatasetFactory.fromConnectorOptions("Jdbc", single);

        assertEquals(1, datasets.size());
        assertEquals("mysql://192.168.10.131:3306", datasets.get(0).namespace());
        assertEquals("ds71.orders", datasets.get(0).name());

        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("table", "customers");
        Map<String, Object> multi = new LinkedHashMap<>();
        multi.put("url", "jdbc:mysql://192.168.10.131:3306/ds72");
        multi.put("table_list", Arrays.asList(entry));

        datasets = LineageDatasetFactory.fromConnectorOptions("Jdbc", multi);

        assertEquals(1, datasets.size());
        assertEquals("mysql://192.168.10.131:3306", datasets.get(0).namespace());
        assertEquals("ds72.customers", datasets.get(0).name());
    }

    @Test
    void emitsNothingForAnUnsupportedConnector() {
        Map<String, Object> options = new LinkedHashMap<>();
        options.put("row.num", 10);

        assertTrue(LineageDatasetFactory.fromConnectorOptions("FakeSource", options).isEmpty());
    }

    private static Map<String, Object> paimonOptions() {
        Map<String, Object> options = new LinkedHashMap<>();
        options.put("warehouse", "s3a://lineage-paimon-warehouse/");
        options.put("catalog_name", "paimon_s3");
        options.put("database", "seatunnel_lineage_verify");
        options.put("table", "st_verify_paimon");
        Map<String, Object> hadoopConf = new LinkedHashMap<>();
        hadoopConf.put("fs.s3a.endpoint", "http://example.invalid");
        options.put("paimon.hadoop.conf", hadoopConf);
        return options;
    }
}
