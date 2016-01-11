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

package org.apache.flink.graph.test.util;

import static org.junit.Assert.assertEquals;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.Utils.ChecksumHashCode;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.test.TestGraphUtils;
import org.apache.flink.graph.utils.GraphUtils;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class GraphUtilsITCase extends MultipleProgramsTestBase {

	public GraphUtilsITCase(TestExecutionMode mode){
		super(mode);
	}

	@Test
	public void testChecksumHashCodeVerticesAndEdges() throws Exception {
		/*
		* Test checksum hashcode
		*/
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Long, Long> graph = Graph.fromDataSet(
			TestGraphUtils.getLongLongVertexData(env),
			TestGraphUtils.getLongLongEdgeData(env),
			env);

		ChecksumHashCode checksum = GraphUtils.checksumHashCode(graph);

		assertEquals(checksum.getCount(), 12L);
		assertEquals(checksum.getChecksum(), 19665L);
    }

}
