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

package org.apache.seatunnel.engine.core.dag.actions;

import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.engine.core.job.ConnectorJarIdentifier;
import org.apache.seatunnel.lineage.LineageDataset;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ObjectStreamClass;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.mockito.Mockito.mock;

class SourceSinkActionCompatibilityTest {

    @Test
    void shouldKeepStableSerialVersionUids() throws Exception {
        Assertions.assertEquals(
                -4104531889750766731L,
                ObjectStreamClass.lookup(SourceAction.class).getSerialVersionUID());
        Assertions.assertEquals(
                -8715419530793414312L,
                ObjectStreamClass.lookup(SinkAction.class).getSerialVersionUID());
        Assertions.assertEquals(
                -4104531889750766731L, serialVersionUidField(SourceAction.class).getLong(null));
        Assertions.assertEquals(
                -8715419530793414312L, serialVersionUidField(SinkAction.class).getLong(null));
    }

    @Test
    void shouldReturnEmptyLineageDatasetsWhenLegacyStreamHasNoField() throws Exception {
        SourceAction<Object, SourceSplit, Serializable> sourceAction =
                new SourceAction<>(
                        1L,
                        "source",
                        mock(SeaTunnelSource.class),
                        emptyJarUrls(),
                        emptyJarIdentifiers());
        SinkAction<Object, Serializable, Serializable, Serializable> sinkAction =
                new SinkAction<>(
                        2L,
                        "sink",
                        mock(SeaTunnelSink.class),
                        emptyJarUrls(),
                        emptyJarIdentifiers());

        setLineageDatasets(sourceAction, null);
        setLineageDatasets(sinkAction, null);

        Assertions.assertEquals(Collections.emptyList(), sourceAction.getLineageDatasets());
        Assertions.assertEquals(Collections.emptyList(), sinkAction.getLineageDatasets());
    }

    @Test
    void shouldCopyConfiguredLineageDatasets() {
        SourceAction<Object, SourceSplit, Serializable> sourceAction =
                new SourceAction<>(
                        1L,
                        "source",
                        mock(SeaTunnelSource.class),
                        emptyJarUrls(),
                        emptyJarIdentifiers());
        List<LineageDataset> configured =
                Collections.singletonList(LineageDataset.of("mysql://host:3306", "db.table"));

        sourceAction.setLineageDatasets(configured);

        Assertions.assertEquals(configured, sourceAction.getLineageDatasets());
        Assertions.assertNotSame(configured, sourceAction.getLineageDatasets());
    }

    private static Field serialVersionUidField(Class<?> type) throws Exception {
        Field field = type.getDeclaredField("serialVersionUID");
        field.setAccessible(true);
        return field;
    }

    private static void setLineageDatasets(Object action, List<LineageDataset> datasets)
            throws Exception {
        Field field = action.getClass().getDeclaredField("lineageDatasets");
        field.setAccessible(true);
        field.set(action, datasets);
    }

    private static Set<URL> emptyJarUrls() {
        return Collections.emptySet();
    }

    private static Set<ConnectorJarIdentifier> emptyJarIdentifiers() {
        return Collections.emptySet();
    }
}
