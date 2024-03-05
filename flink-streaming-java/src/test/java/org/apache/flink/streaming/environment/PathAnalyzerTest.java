package org.apache.flink.streaming.environment;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.streaming.api.environment.PathAnalyzer;
import org.apache.flink.streaming.api.environment.PathTrackerOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;

import org.junit.Test;

import java.util.Collections;

public class PathAnalyzerTest {
    private static final String EXEC_NAME = "test-executor";



    @Test
    public void pathComputationTest() throws Exception {
        final Configuration configuration = new Configuration();
        configuration.set(DeploymentOptions.TARGET, EXEC_NAME);
        configuration.set(PathTrackerOptions.ENABLE, true);


        final StreamExecutionEnvironment env = new StreamExecutionEnvironment(configuration);
        env.fromData(Collections.singletonList(123)).setParallelism(1)
                .map( (x) -> {return 2*x;})
                .setParallelism(3)
                .rebalance()

                .map( (x) -> {return x*x;})
                .setParallelism(4)
                .addSink(new DiscardingSink<>());

        int pathNum = PathAnalyzer.computePathNum(env);
        assert (pathNum == 12);

    }


}
