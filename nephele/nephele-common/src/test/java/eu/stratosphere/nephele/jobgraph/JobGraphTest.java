/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

/*
 *  Copyright 2010 casp.
 * 
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 * 
 *       http://www.apache.org/licenses/LICENSE-2.0
 * 
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *  under the License.
 */

package eu.stratosphere.nephele.jobgraph;

import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;

import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author casp
 */
public class JobGraphTest {

	public JobGraphTest() {
	}

	@BeforeClass
	public static void setUpClass() throws Exception {
	}

	@AfterClass
	public static void tearDownClass() throws Exception {
	}

	@Before
	public void setUp() {
	}

	@After
	public void tearDown() {
	}

	@Test
	public void testJobGraph() {
		// check if the backward edge really points to the preceding vertex
		final JobGraph jg = new JobGraph();

		final JobTaskVertex v1 = new JobTaskVertex(jg);
		final JobTaskVertex v2 = new JobTaskVertex(jg);

		try {
			v1.connectTo(v2);
		} catch (JobGraphDefinitionException ex) {
			Logger.getLogger(JobGraphTest.class.getName()).log(Level.SEVERE, null, ex);
		}

		assertEquals(v1, v2.getBackwardConnection(0).getConnectedVertex());

	}

	/**
	 * In this test we construct a job graph and set the dependency chain for instance sharing in a way that a cycle is
	 * created. The test is considered successful if the cycle is detected.
	 */
	@Test
	public void detectCycleInInstanceSharingDependencyChain() {

		final JobGraph jg = new JobGraph();

		final JobTaskVertex v1 = new JobTaskVertex("v1", jg);
		final JobTaskVertex v2 = new JobTaskVertex("v2", jg);
		final JobTaskVertex v3 = new JobTaskVertex("v3", jg);
		final JobTaskVertex v4 = new JobTaskVertex("v4", jg);

		try {
			v1.connectTo(v2);
			v2.connectTo(v3);
			v3.connectTo(v4);
		} catch (JobGraphDefinitionException ex) {
			Logger.getLogger(JobGraphTest.class.getName()).log(Level.SEVERE, null, ex);
		}

		// Dependency chain is acyclic
		v1.setVertexToShareInstancesWith(v2);
		v3.setVertexToShareInstancesWith(v2);
		v4.setVertexToShareInstancesWith(v1);

		assertEquals(jg.isInstanceDependencyChainAcyclic(), true);

		// Create a cycle v4 -> v1 -> v2 -> v4
		v2.setVertexToShareInstancesWith(v4);

		assertEquals(jg.isInstanceDependencyChainAcyclic(), false);
	}

	// TODO add test methods here.
	// The methods must be annotated with annotation @Test. For example:
	//
	// @Test
	// public void hello() {}

}