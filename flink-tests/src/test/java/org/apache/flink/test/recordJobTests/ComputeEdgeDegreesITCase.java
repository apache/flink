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


package org.apache.flink.test.recordJobTests;

import java.util.Collection;

import org.apache.flink.api.common.Plan;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.recordJobs.graph.ComputeEdgeDegrees;
import org.apache.flink.test.util.RecordAPITestBase;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class ComputeEdgeDegreesITCase extends RecordAPITestBase {
	
	protected String edgesPath = null;
	protected String resultPath = null; 

	private static final String EDGES = "1,2\n1,3\n1,4\n1,5\n2,3\n2,5\n3,4\n3,7\n4,3\n6,5\n8,3\n7,8\n5,6\n";
	private static final String EXPECTED = "1,4|2,3\n1,4|3,5\n1,4|4,2\n1,4|5,3\n2,3|3,5\n2,3|5,3\n3,5|4,2\n3,5|7,2\n5,3|6,1\n3,5|8,2\n7,2|8,2\n";
	
	public ComputeEdgeDegreesITCase(Configuration config) {
		super(config);
		setTaskManagerNumSlots(parallelism);
	}

	@Override
	protected void preSubmit() throws Exception {
		edgesPath = createTempFile("edges.txt", EDGES);
		resultPath = getTempDirPath("edgesWithDegrees");
	}

	@Override
	protected Plan getTestJob() {
		ComputeEdgeDegrees computeDegrees = new ComputeEdgeDegrees();
		return computeDegrees.getPlan(String.valueOf(config.getInteger("NumSubtasks", 4)),
				edgesPath, resultPath);
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(EXPECTED, resultPath);
	}

	@Parameters
	public static Collection<Object[]> getConfigurations() {
		Configuration config = new Configuration();
		config.setInteger("NumSubtasks", parallelism);
		return toParameterList(config);
	}
}
