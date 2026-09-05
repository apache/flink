/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.lineage;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;

/** Serialization tests for lineage classes made serializable. */
class LineageSerializationTest {

    @Test
    void testDefaultLineageVertexSerialization() throws Exception {
        DefaultLineageDataset dataset =
                new DefaultLineageDataset("ds", "ns", Collections.emptyMap());
        DefaultLineageVertex vertex = new DefaultLineageVertex();
        vertex.addLineageDataset(dataset);

        DefaultLineageVertex cloned = InstantiationUtil.clone(vertex);

        assertThat(cloned.datasets()).hasSize(1);
        assertThat(cloned.datasets().get(0).name()).isEqualTo("ds");
        assertThat(cloned.datasets().get(0).namespace()).isEqualTo("ns");
    }

    @Test
    void testDefaultSourceLineageVertexSerialization() throws Exception {
        DefaultLineageDataset dataset =
                new DefaultLineageDataset("source-ds", "source-ns", Collections.emptyMap());
        DefaultSourceLineageVertex vertex =
                new DefaultSourceLineageVertex(Boundedness.CONTINUOUS_UNBOUNDED);
        vertex.addDataset(dataset);

        DefaultSourceLineageVertex cloned = InstantiationUtil.clone(vertex);

        assertThat(cloned.boundedness()).isEqualTo(Boundedness.CONTINUOUS_UNBOUNDED);
        assertThat(cloned.datasets()).hasSize(1);
        assertThat(cloned.datasets().get(0).name()).isEqualTo("source-ds");
        assertThat(cloned.datasets().get(0).namespace()).isEqualTo("source-ns");
    }

    @Test
    void testDefaultTypeDatasetFacetSerialization() throws Exception {
        DefaultTypeDatasetFacet facet = new DefaultTypeDatasetFacet(BasicTypeInfo.LONG_TYPE_INFO);

        DefaultTypeDatasetFacet cloned = InstantiationUtil.clone(facet);

        assertThat(cloned.name()).isEqualTo(DefaultTypeDatasetFacet.TYPE_FACET_NAME);
        assertThat(cloned.getTypeInformation()).isEqualTo(BasicTypeInfo.LONG_TYPE_INFO);
    }

    @Test
    void testDefaultLineageDatasetWithFacetSerialization() throws Exception {
        DefaultTypeDatasetFacet facet = new DefaultTypeDatasetFacet(BasicTypeInfo.STRING_TYPE_INFO);
        HashMap<String, LineageDatasetFacet> facets = new HashMap<>();
        facets.put(facet.name(), facet);
        DefaultLineageDataset dataset = new DefaultLineageDataset("ds", "ns", facets);

        DefaultLineageDataset cloned = InstantiationUtil.clone(dataset);

        assertThat(cloned.name()).isEqualTo("ds");
        assertThat(cloned.namespace()).isEqualTo("ns");
        assertThat(cloned.facets()).hasSize(1);
        assertThat(cloned.facets().get(DefaultTypeDatasetFacet.TYPE_FACET_NAME)).isEqualTo(facet);
    }
}
