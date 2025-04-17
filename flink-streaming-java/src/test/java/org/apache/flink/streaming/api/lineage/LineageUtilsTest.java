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

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.Boundedness;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Testing for lineage util. */
public class LineageUtilsTest {
    private static final String TEST_NAME = "testName";
    private static final String TEST_NAMESPACE = "testNameSpace";

    @Test
    public void testDataSetOf() {
        DefaultTypeDatasetFacet typeDatasetFacet = new DefaultTypeDatasetFacet(Types.BIG_INT);
        LineageDataset dataset =
                LineageUtils.datasetOf(TEST_NAME, TEST_NAMESPACE, typeDatasetFacet);

        assertThat(dataset.name()).isEqualTo(TEST_NAME);
        assertThat(dataset.namespace()).isEqualTo(TEST_NAMESPACE);
        assertThat(dataset.facets()).size().isEqualTo(1);
        assertThat(dataset.facets().get(typeDatasetFacet.name())).isEqualTo(typeDatasetFacet);
    }

    @Test
    public void testSourceLineageVertexOf() {
        LineageDataset dataset =
                LineageUtils.datasetOf(
                        TEST_NAME, TEST_NAMESPACE, new DefaultTypeDatasetFacet(Types.BIG_INT));
        SourceLineageVertex sourceLineageVertex =
                LineageUtils.sourceLineageVertexOf(Boundedness.CONTINUOUS_UNBOUNDED, dataset);

        assertThat(sourceLineageVertex.boundedness()).isEqualTo(Boundedness.CONTINUOUS_UNBOUNDED);
        assertThat(sourceLineageVertex.datasets()).containsExactly(dataset);
    }

    @Test
    public void testLineageVertexOf() {
        LineageDataset dataset =
                LineageUtils.datasetOf(
                        TEST_NAME, TEST_NAMESPACE, new DefaultTypeDatasetFacet(Types.BIG_INT));
        LineageVertex lineageVertex = LineageUtils.lineageVertexOf(dataset);
        assertThat(lineageVertex.datasets()).containsExactly(dataset);
    }
}
