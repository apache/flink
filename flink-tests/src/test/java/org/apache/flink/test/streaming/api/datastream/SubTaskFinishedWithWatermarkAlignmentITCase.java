package org.apache.flink.test.streaming.api.datastream;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.Test;

import java.time.Duration;
import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

/** This ITCase class tests the behavior of task execution with watermark alignment. */
public class SubTaskFinishedWithWatermarkAlignmentITCase {

    /**
     * Test method to verify whether the watermark alignment works well with finished task.
     *
     * @throws Exception if any error occurs during the execution.
     */
    @Test
    public void testTaskFinishedWithWatermarkAlignmentExecution() throws Exception {
        // Set up the execution environment with parallelism of 2
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // Create a stream from a custom source with watermark strategy
        DataStream<Long> stream =
                env.fromSource(
                                new NumberSequenceSource(0, 100),
                                WatermarkStrategy.<Long>forMonotonousTimestamps()
                                        .withTimestampAssigner(
                                                (SerializableTimestampAssigner<Long>)
                                                        (aLong, l) -> aLong)
                                        .withWatermarkAlignment(
                                                "g1", Duration.ofMillis(10), Duration.ofSeconds(2)),
                                "Sequence Source")
                        .filter((FilterFunction<Long>) aLong -> true);

        // Execute the stream and collect the results
        final List<Long> result = stream.executeAndCollect(101);

        // Assert that the collected result contains all numbers from 0 to 100 in any order
        assertThat(result, containsInAnyOrder(LongStream.rangeClosed(0, 100).boxed().toArray()));
    }
}
