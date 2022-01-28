package org.apache.flink.streaming.tests;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.tests.util.TestUtils;
import org.apache.flink.tests.util.flink.ClusterController;
import org.apache.flink.tests.util.flink.FlinkResource;
import org.apache.flink.tests.util.flink.FlinkResourceSetup;
import org.apache.flink.tests.util.flink.JobSubmission;
import org.apache.flink.tests.util.flink.LocalStandaloneFlinkResourceFactory;

import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;

/** Nary Input End-to-End test. */
@RunWith(Parameterized.class)
public class NaryInputTest {

    private static class Params {
        public int numOfTotal;
        public int numOfIdle;

        public Params(int numOfTotal, int numOfIdle) {
            this.numOfTotal = numOfTotal;
            this.numOfIdle = numOfIdle;
        }
    }

    @Parameterized.Parameters(name = "{index}: numOfTotal: {0} numOfIdle: {1}")
    public static Collection<Params> parameters() {
        return Arrays.asList(
                new Params(2, 0),
                new Params(2, 1),
                new Params(5, 0),
                new Params(5, 2),
                new Params(5, 5),
                new Params(10, 0),
                new Params(10, 5),
                new Params(10, 9));
    }

    @Parameterized.Parameter public Params param;

    private static Configuration getConfiguration() {
        // we have to enable checkpoint to trigger flushing for filesystem sink
        final Configuration flinkConfig = new Configuration();
        /**
         * -Dtaskmanager.memory.jvm-overhead.fraction=0.1 \
         * -Dtaskmanager.memory.network.fraction=0.2 \ -Dtaskmanager.memory.managed.fraction=0.01 \
         */
        MemorySize processMem = MemorySize.ofMebiBytes(192);
        MemorySize metaspaceMem = MemorySize.ofMebiBytes(128);
        MemorySize overheadMem = MemorySize.ofMebiBytes(32);
        MemorySize flinkMem = processMem.subtract(metaspaceMem).subtract(overheadMem);
        MemorySize jvmHeap = flinkMem.multiply(0.4);
        MemorySize taskHeap = jvmHeap.multiply(0.8);
        MemorySize frameworkHeap = jvmHeap.multiply(0.2);
        MemorySize offHeap = flinkMem.multiply(0.6);
        MemorySize managedMem = offHeap.multiply(0.1);
        MemorySize directMem = offHeap.multiply(0.9);
        MemorySize networkMem = directMem.multiply(0.8);
        MemorySize frameworkOffHeapMem = directMem.multiply(.1);
        MemorySize taskOffHeapMem = directMem.multiply(.1);

        flinkConfig.setString("taskmanager.memory.process.size", processMem.toString());
        flinkConfig.setString("taskmanager.memory.flink.size", flinkMem.toString());
        flinkConfig.setString("taskmanager.memory.task.heap.size", taskHeap.toString());
        flinkConfig.setString("taskmanager.memory.managed.size", managedMem.toString());
        flinkConfig.setString("taskmanager.memory.framework.heap.size", frameworkHeap.toString());
        flinkConfig.setString(
                "taskmanager.memory.framework.off-heap.size", frameworkOffHeapMem.toString());
        flinkConfig.setString("taskmanager.memory.task.off-heap.size", taskOffHeapMem.toString());
        flinkConfig.setString("taskmanager.memory.jvm-overhead.fraction", "0.1");
        flinkConfig.setString("taskmanager.memory.jvm-overhead.min", "16m");
        flinkConfig.setString("taskmanager.memory.jvm-metaspace.size", metaspaceMem.toString());
        flinkConfig.setString("taskmanager.memory.network.size", networkMem.toString());
        flinkConfig.setString("taskmanager.memory.network.min", "1 bytes");
        flinkConfig.setInteger("taskmanager.numberOfTaskSlots", 1);
        flinkConfig.setString("restart-strategy", "none");
        return flinkConfig;
    }

    @Rule
    public final FlinkResource flink =
            new LocalStandaloneFlinkResourceFactory()
                    .create(
                            FlinkResourceSetup.builder()
                                    .addConfiguration(getConfiguration())
                                    .build());

    private ClusterController cluster;

    @BeforeEach
    public void init() throws Exception {
        cluster = flink.startCluster(1);
    }

    @AfterEach
    public void tearDown() throws Exception {
        cluster.close();
    }

    @Test
    public void testNaryInput() throws Exception {
        final Path jobJar = TestUtils.getResource("/jobs.jar");

        try (final ClusterController clusterController = flink.startCluster(1)) {
            // if the job fails then this throws an exception
            final Duration runFor = Duration.ofMinutes(1);
            Assertions.assertDoesNotThrow(
                    () -> {
                        clusterController.submitJob(
                                new JobSubmission.JobSubmissionBuilder(jobJar)
                                        .setDetached(false)
                                        .addArgument("--limit", String.valueOf(1_000_000))
                                        .addArgument(
                                                "--num-of-total", String.valueOf(param.numOfTotal))
                                        .addArgument(
                                                "--num-of-idle", String.valueOf(param.numOfIdle))
                                        .setMainClass(
                                                "org.apache.flink.streaming.tests.NaryInputJob")
                                        .build(),
                                runFor.plusSeconds(30));
                    },
                    "Job should not fail.");
        }
    }
}
