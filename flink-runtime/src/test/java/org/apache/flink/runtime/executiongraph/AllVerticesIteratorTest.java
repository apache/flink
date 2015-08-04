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

package org.apache.flink.runtime.executiongraph;

import java.util.Arrays;

import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class AllVerticesIteratorTest {

	@Test
	public void testAllVertices() {
		try {
			
			JobVertex v1 = new JobVertex("v1");
			JobVertex v2 = new JobVertex("v2");
			JobVertex v3 = new JobVertex("v3");
			JobVertex v4 = new JobVertex("v4");
			
			v1.setParallelism(1);
			v2.setParallelism(7);
			v3.setParallelism(3);
			v4.setParallelism(2);
			
			ExecutionGraph eg = Mockito.mock(ExecutionGraph.class);
			Mockito.when(eg.getExecutionContext()).thenReturn(TestingUtils.directExecutionContext());
					
			ExecutionJobVertex ejv1 = new ExecutionJobVertex(eg, v1, 1,
					AkkaUtils.getDefaultTimeout());
			ExecutionJobVertex ejv2 = new ExecutionJobVertex(eg, v2, 1,
					AkkaUtils.getDefaultTimeout());
			ExecutionJobVertex ejv3 = new ExecutionJobVertex(eg, v3, 1,
					AkkaUtils.getDefaultTimeout());
			ExecutionJobVertex ejv4 = new ExecutionJobVertex(eg, v4, 1,
					AkkaUtils.getDefaultTimeout());
			
			AllVerticesIterator iter = new AllVerticesIterator(Arrays.asList(ejv1, ejv2, ejv3, ejv4).iterator());
			
			int numReturned = 0;
			while (iter.hasNext()) {
				iter.hasNext();
				Assert.assertNotNull(iter.next());
				numReturned++;
			}
			
			Assert.assertEquals(13, numReturned);
		}
		catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}
}
