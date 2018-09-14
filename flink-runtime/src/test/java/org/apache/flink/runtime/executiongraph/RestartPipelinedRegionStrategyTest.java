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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.blob.VoidBlobWriter;
import org.apache.flink.runtime.executiongraph.failover.FailoverRegion;
import org.apache.flink.runtime.executiongraph.failover.RestartPipelinedRegionStrategy;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import org.apache.flink.runtime.testingUtils.TestingUtils;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class RestartPipelinedRegionStrategyTest {

	/**
	 * Creates a JobGraph of the following form:
	 * 
	 * <pre>
	 *  v1--->v2-->\
	 *              \
	 *               v4 --->\
	 *        ----->/        \
	 *  v3-->/                v5
	 *       \               /
	 *        ------------->/
	 * </pre>
	 */
	@Test
	public void testSimpleFailoverRegion() throws Exception {
		
		final JobID jobId = new JobID();
		final String jobName = "Test Job Sample Name";

		JobVertex v1 = new JobVertex("vertex1");
		JobVertex v2 = new JobVertex("vertex2");
		JobVertex v3 = new JobVertex("vertex3");
		JobVertex v4 = new JobVertex("vertex4");
		JobVertex v5 = new JobVertex("vertex5");
		
		v1.setParallelism(5);
		v2.setParallelism(7);
		v3.setParallelism(2);
		v4.setParallelism(11);
		v5.setParallelism(4);

		v1.setInvokableClass(AbstractInvokable.class);
		v2.setInvokableClass(AbstractInvokable.class);
		v3.setInvokableClass(AbstractInvokable.class);
		v4.setInvokableClass(AbstractInvokable.class);
		v5.setInvokableClass(AbstractInvokable.class);

		v2.connectNewDataSetAsInput(v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		v4.connectNewDataSetAsInput(v2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		v4.connectNewDataSetAsInput(v3, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		v5.connectNewDataSetAsInput(v4, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		v5.connectNewDataSetAsInput(v3, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		
		List<JobVertex> ordered = new ArrayList<JobVertex>(Arrays.asList(v1, v2, v3, v4, v5));

        Scheduler scheduler = new Scheduler(TestingUtils.defaultExecutor());
        final JobInformation jobInformation = new DummyJobInformation(
			jobId,
			jobName);

		ExecutionGraph eg = new ExecutionGraph(
			jobInformation,
			TestingUtils.defaultExecutor(),
			TestingUtils.defaultExecutor(),
			AkkaUtils.getDefaultTimeout(),
			new NoRestartStrategy(),
            new RestartPipelinedRegionStrategy.Factory(),
            scheduler,
            ExecutionGraph.class.getClassLoader(),
			VoidBlobWriter.getInstance(),
			AkkaUtils.getDefaultTimeout());
		try {
			eg.attachJobGraph(ordered);
		}
		catch (JobException e) {
			e.printStackTrace();
			fail("Job failed with exception: " + e.getMessage());
		}

        RestartPipelinedRegionStrategy strategy = (RestartPipelinedRegionStrategy)eg.getFailoverStrategy();
        ExecutionJobVertex ejv1 = eg.getJobVertex(v1.getID());
        ExecutionJobVertex ejv2 = eg.getJobVertex(v2.getID());
        ExecutionJobVertex ejv3 = eg.getJobVertex(v3.getID());
        ExecutionJobVertex ejv4 = eg.getJobVertex(v4.getID());
        ExecutionJobVertex ejv5 = eg.getJobVertex(v5.getID());
        FailoverRegion region1 = strategy.getFailoverRegion(ejv1.getTaskVertices()[2]);
        FailoverRegion region2 = strategy.getFailoverRegion(ejv2.getTaskVertices()[3]);
        FailoverRegion region3 = strategy.getFailoverRegion(ejv3.getTaskVertices()[0]);
        FailoverRegion region4 = strategy.getFailoverRegion(ejv4.getTaskVertices()[4]);
        FailoverRegion region5 = strategy.getFailoverRegion(ejv5.getTaskVertices()[1]);

        assertEquals(region1, region2);
        assertEquals(region3, region2);
        assertEquals(region4, region2);
        assertEquals(region5, region2);
	}

    /**
     * Creates a JobGraph of the following form:
     *
     * <pre>
     *  v2 ------->\
     *              \
     *  v1---------> v4 --->|\
     *                        \
     *                        v5
     *                       /
     *  v3--------------->|/
     * </pre>
     */
	@Test
	public void testMultipleFailoverRegions() throws Exception {
		final JobID jobId = new JobID();
		final String jobName = "Test Job Sample Name";

        JobVertex v1 = new JobVertex("vertex1");
        JobVertex v2 = new JobVertex("vertex2");
        JobVertex v3 = new JobVertex("vertex3");
        JobVertex v4 = new JobVertex("vertex4");
        JobVertex v5 = new JobVertex("vertex5");

        v1.setParallelism(3);
        v2.setParallelism(2);
        v3.setParallelism(2);
        v4.setParallelism(5);
        v5.setParallelism(2);

        v1.setInvokableClass(AbstractInvokable.class);
        v2.setInvokableClass(AbstractInvokable.class);
        v3.setInvokableClass(AbstractInvokable.class);
        v4.setInvokableClass(AbstractInvokable.class);
        v5.setInvokableClass(AbstractInvokable.class);

        v4.connectNewDataSetAsInput(v2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
        v4.connectNewDataSetAsInput(v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
        v5.connectNewDataSetAsInput(v4, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);
        v5.connectNewDataSetAsInput(v3, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

        List<JobVertex> ordered = new ArrayList<JobVertex>(Arrays.asList(v1, v2, v3, v4, v5));

        Scheduler scheduler = new Scheduler(TestingUtils.defaultExecutor());
        final JobInformation jobInformation = new DummyJobInformation(
			jobId,
			jobName);

		ExecutionGraph eg = new ExecutionGraph(
			jobInformation,
			TestingUtils.defaultExecutor(),
			TestingUtils.defaultExecutor(),
			AkkaUtils.getDefaultTimeout(),
			new NoRestartStrategy(),
            new RestartPipelinedRegionStrategy.Factory(),
            scheduler,
            ExecutionGraph.class.getClassLoader(),
			VoidBlobWriter.getInstance(),
			AkkaUtils.getDefaultTimeout());
		try {
			eg.attachJobGraph(ordered);
		}
		catch (JobException e) {
			e.printStackTrace();
			fail("Job failed with exception: " + e.getMessage());
		}

        // All in one failover region
        RestartPipelinedRegionStrategy strategy = (RestartPipelinedRegionStrategy)eg.getFailoverStrategy();
        ExecutionJobVertex ejv1 = eg.getJobVertex(v1.getID());
        ExecutionJobVertex ejv2 = eg.getJobVertex(v2.getID());
        ExecutionJobVertex ejv3 = eg.getJobVertex(v3.getID());
        ExecutionJobVertex ejv4 = eg.getJobVertex(v4.getID());
        ExecutionJobVertex ejv5 = eg.getJobVertex(v5.getID());
        FailoverRegion region1 = strategy.getFailoverRegion(ejv1.getTaskVertices()[1]);
        FailoverRegion region2 = strategy.getFailoverRegion(ejv2.getTaskVertices()[0]);
        FailoverRegion region4 = strategy.getFailoverRegion(ejv4.getTaskVertices()[3]);
        FailoverRegion region31 = strategy.getFailoverRegion(ejv3.getTaskVertices()[0]);
        FailoverRegion region32 = strategy.getFailoverRegion(ejv3.getTaskVertices()[1]);
        FailoverRegion region51 = strategy.getFailoverRegion(ejv5.getTaskVertices()[0]);
        FailoverRegion region52 = strategy.getFailoverRegion(ejv5.getTaskVertices()[1]);

        //There should be 5 failover regions. v1 v2 v4 in one, v3 has two, v5 has two
        assertEquals(region1, region2);
        assertEquals(region2, region4);
        assertFalse(region31.equals(region32));
        assertFalse(region51.equals(region52));
	}

    /**
     * Creates a JobGraph of the following form:
     *
     * <pre>
     *  v1--->v2-->\
     *              \
     *               v4 --->|\
     *        ----->/        \
     *  v3-->/                v5
     *       \               /
     *        ------------->/
     * </pre>
     */
	@Test
	public void testSingleRegionWithMixedInput() throws Exception {
		final JobID jobId = new JobID();
		final String jobName = "Test Job Sample Name";

        JobVertex v1 = new JobVertex("vertex1");
        JobVertex v2 = new JobVertex("vertex2");
        JobVertex v3 = new JobVertex("vertex3");
        JobVertex v4 = new JobVertex("vertex4");
        JobVertex v5 = new JobVertex("vertex5");

        v1.setParallelism(3);
        v2.setParallelism(2);
        v3.setParallelism(2);
        v4.setParallelism(5);
        v5.setParallelism(2);

        v1.setInvokableClass(AbstractInvokable.class);
        v2.setInvokableClass(AbstractInvokable.class);
        v3.setInvokableClass(AbstractInvokable.class);
        v4.setInvokableClass(AbstractInvokable.class);
        v5.setInvokableClass(AbstractInvokable.class);

        v2.connectNewDataSetAsInput(v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
        v4.connectNewDataSetAsInput(v2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
        v4.connectNewDataSetAsInput(v3, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
        v5.connectNewDataSetAsInput(v3, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
        v5.connectNewDataSetAsInput(v4, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

        List<JobVertex> ordered = new ArrayList<JobVertex>(Arrays.asList(v1, v2, v3, v4, v5));

        Scheduler scheduler = new Scheduler(TestingUtils.defaultExecutor());
		final JobInformation jobInformation = new DummyJobInformation(
			jobId,
			jobName);

		ExecutionGraph eg = new ExecutionGraph(
			jobInformation,
			TestingUtils.defaultExecutor(),
			TestingUtils.defaultExecutor(),
			AkkaUtils.getDefaultTimeout(),
			new NoRestartStrategy(),
            new RestartPipelinedRegionStrategy.Factory(),
            scheduler,
            ExecutionGraph.class.getClassLoader(),
			VoidBlobWriter.getInstance(),
			AkkaUtils.getDefaultTimeout());
		try {
			eg.attachJobGraph(ordered);
		}
		catch (JobException e) {
			e.printStackTrace();
			fail("Job failed with exception: " + e.getMessage());
		}

        // All in one failover region
        RestartPipelinedRegionStrategy strategy = (RestartPipelinedRegionStrategy)eg.getFailoverStrategy();
        ExecutionJobVertex ejv1 = eg.getJobVertex(v1.getID());
        ExecutionJobVertex ejv2 = eg.getJobVertex(v2.getID());
        ExecutionJobVertex ejv3 = eg.getJobVertex(v3.getID());
        ExecutionJobVertex ejv4 = eg.getJobVertex(v4.getID());
        ExecutionJobVertex ejv5 = eg.getJobVertex(v5.getID());
        FailoverRegion region1 = strategy.getFailoverRegion(ejv1.getTaskVertices()[1]);
        FailoverRegion region2 = strategy.getFailoverRegion(ejv2.getTaskVertices()[0]);
        FailoverRegion region4 = strategy.getFailoverRegion(ejv4.getTaskVertices()[3]);
        FailoverRegion region3 = strategy.getFailoverRegion(ejv3.getTaskVertices()[0]);
        FailoverRegion region5 = strategy.getFailoverRegion(ejv5.getTaskVertices()[1]);

        assertEquals(region1, region2);
        assertEquals(region2, region4);
        assertEquals(region3, region2);
        assertEquals(region1, region5);
    }

    /**
     * Creates a JobGraph of the following form:
     *
     * <pre>
     *  v1-->v2-->|\
     *              \
     *               v4
     *             /
     *  v3------>/
     * </pre>
     */
	@Test
	public void testMultiRegionNotAllToAll() throws Exception {
		final JobID jobId = new JobID();
		final String jobName = "Test Job Sample Name";

        JobVertex v1 = new JobVertex("vertex1");
        JobVertex v2 = new JobVertex("vertex2");
        JobVertex v3 = new JobVertex("vertex3");
        JobVertex v4 = new JobVertex("vertex4");
        JobVertex v5 = new JobVertex("vertex5");

        v1.setParallelism(2);
        v2.setParallelism(2);
        v3.setParallelism(5);
        v4.setParallelism(5);

        v1.setInvokableClass(AbstractInvokable.class);
        v2.setInvokableClass(AbstractInvokable.class);
        v3.setInvokableClass(AbstractInvokable.class);
        v4.setInvokableClass(AbstractInvokable.class);

        v2.connectNewDataSetAsInput(v1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
        v4.connectNewDataSetAsInput(v2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);
        v4.connectNewDataSetAsInput(v3, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

        List<JobVertex> ordered = new ArrayList<JobVertex>(Arrays.asList(v1, v2, v3, v4));

        Scheduler scheduler = new Scheduler(TestingUtils.defaultExecutor());
		final JobInformation jobInformation = new DummyJobInformation(
			jobId,
			jobName);

        ExecutionGraph eg = new ExecutionGraph(
        	jobInformation,
			TestingUtils.defaultExecutor(),
			TestingUtils.defaultExecutor(),
			AkkaUtils.getDefaultTimeout(),
			new NoRestartStrategy(),
            new RestartPipelinedRegionStrategy.Factory(),
            scheduler,
            ExecutionGraph.class.getClassLoader(),
			VoidBlobWriter.getInstance(),
			AkkaUtils.getDefaultTimeout());
		try {
			eg.attachJobGraph(ordered);
		}
		catch (JobException e) {
			e.printStackTrace();
			fail("Job failed with exception: " + e.getMessage());
		}

        // All in one failover region
        RestartPipelinedRegionStrategy strategy = (RestartPipelinedRegionStrategy)eg.getFailoverStrategy();
        ExecutionJobVertex ejv1 = eg.getJobVertex(v1.getID());
        ExecutionJobVertex ejv2 = eg.getJobVertex(v2.getID());
        ExecutionJobVertex ejv3 = eg.getJobVertex(v3.getID());
        ExecutionJobVertex ejv4 = eg.getJobVertex(v4.getID());
        FailoverRegion region11 = strategy.getFailoverRegion(ejv1.getTaskVertices()[0]);
        FailoverRegion region12 = strategy.getFailoverRegion(ejv1.getTaskVertices()[1]);
        FailoverRegion region21 = strategy.getFailoverRegion(ejv2.getTaskVertices()[0]);
        FailoverRegion region22 = strategy.getFailoverRegion(ejv2.getTaskVertices()[1]);
        FailoverRegion region3 = strategy.getFailoverRegion(ejv3.getTaskVertices()[0]);
        FailoverRegion region4 = strategy.getFailoverRegion(ejv4.getTaskVertices()[3]);

        //There should be 3 failover regions. v1 v2 in two, v3 and v4 in one
        assertEquals(region11, region21);
        assertEquals(region12, region22);
        assertFalse(region11.equals(region12));
        assertFalse(region3.equals(region4));
	}

}
