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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.SubmittedJobGraph;
import org.apache.flink.util.FlinkException;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;

/**
 * Test {@link SingleJobSubmittedJobGraphStore} functionality.
 */
public class SingleJobSubmittedJobGraphStoreTest {

	@Rule public ExpectedException expectedException = ExpectedException.none();

	private static SingleJobSubmittedJobGraphStore singleJobSubmittedJobGraphStore;
	private static JobID thisJobID;
	private static JobGraph thisJobGraph;
	private static JobID otherJobID;

	@BeforeClass
	public static void init() {
		thisJobID = new JobID(100, 100);
		otherJobID = new JobID(999, 999);

		thisJobGraph = new JobGraph(thisJobID, "thisJobGraph");
		singleJobSubmittedJobGraphStore = new SingleJobSubmittedJobGraphStore(thisJobGraph);
	}

	@Test
	public void testPutJobGraph() throws Exception {
		assertEquals(1, singleJobSubmittedJobGraphStore.getJobIds().size());

		SubmittedJobGraph otherSubmittedJobGraph = new SubmittedJobGraph(new JobGraph(otherJobID, "otherJobGraph"));
		//no-op
		singleJobSubmittedJobGraphStore.putJobGraph(new SubmittedJobGraph(thisJobGraph));

		expectedException.expect(FlinkException.class);
		singleJobSubmittedJobGraphStore.putJobGraph(otherSubmittedJobGraph);
	}

	@Test
	public void testRecoverJobGraph() throws Exception {
		SubmittedJobGraph submittedJobGraph = singleJobSubmittedJobGraphStore.recoverJobGraph(thisJobID);

		assertEquals(1, singleJobSubmittedJobGraphStore.getJobIds().size());
		assertEquals(submittedJobGraph.getJobId(), thisJobID);

		expectedException.expect(FlinkException.class);
		singleJobSubmittedJobGraphStore.recoverJobGraph(otherJobID);
	}

	@Test
	public void testRemoveJobGraph() {
		assertEquals(1, singleJobSubmittedJobGraphStore.getJobIds().size());
		//no-op
		singleJobSubmittedJobGraphStore.removeJobGraph(thisJobID);
		assertEquals(1, singleJobSubmittedJobGraphStore.getJobIds().size());
	}
}
