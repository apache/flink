package org.apache.flink.streaming.environment;


import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;

import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;

import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.streaming.api.environment.PathTrackerOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.environment.TestingDefaultExecutionGraphBuilder;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;


import org.apache.flink.testutils.TestingUtils;

import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;

import static org.junit.Assert.fail;

public class PathTrackerTest {
    private static final String EXEC_NAME = "test-executor";

    @Test
    public void pathTrackerConfigTest() throws Exception {
        final Configuration configuration = new Configuration();
        configuration.set(DeploymentOptions.TARGET, EXEC_NAME);
        configuration.set(PathTrackerOptions.ENABLE, true);

        final StreamExecutionEnvironment env = new StreamExecutionEnvironment(configuration);
        env.fromData(Collections.singletonList(123)).sinkTo(new DiscardingSink<>());
//        env.computePathNum();

    }


    @Test
    public void pathComputationTest() throws Exception {
        final Configuration configuration = new Configuration();
        configuration.set(DeploymentOptions.TARGET, EXEC_NAME);
        configuration.set(PathTrackerOptions.ENABLE, true);


        final StreamExecutionEnvironment env = new StreamExecutionEnvironment(configuration);
        env.fromData(Collections.singletonList(123)).setParallelism(1)
                .map( (x) -> {return 2*x;})
                .setParallelism(8)
                .rescale()

                .map( (x) -> {return x*x;})
                .setParallelism(2)
                .sinkTo(new DiscardingSink<>());

//        System.out.println(env.computePathNum());

        JobGraph jobGraph = env.getStreamGraph(false).getJobGraph();

        ExecutionGraph eg =
               TestingDefaultExecutionGraphBuilder.newBuilder()
                        .setVertexParallelismStore(
                                SchedulerBase.computeVertexParallelismStore(jobGraph))
                        .build(Executors.newSingleThreadScheduledExecutor());
        List<JobVertex> jobVertices = jobGraph.getVerticesSortedTopologicallyFromSources();
        System.out.println(jobVertices.size());
        try {
            eg.attachJobGraph(
                    jobVertices,
                    UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup());
        } catch (JobException e) {
            e.printStackTrace();
            fail("Building ExecutionGraph failed: " + e.getMessage());
        }


        JobVertex mapVertex2 = jobVertices.get(2);
        ExecutionJobVertex execMapVertex = eg.getJobVertex(mapVertex2.getID());
        ExecutionVertex[] mapTaskVertices = execMapVertex.getTaskVertices();
        for (ExecutionVertex mapTaskVertex : mapTaskVertices) {
            IntermediateResultPartitionID consumedPartitionId =
                    mapTaskVertex.getConsumedPartitionGroup(0).getFirst();
        }
        System.out.println(execMapVertex.getInputs().size());
    }


}
