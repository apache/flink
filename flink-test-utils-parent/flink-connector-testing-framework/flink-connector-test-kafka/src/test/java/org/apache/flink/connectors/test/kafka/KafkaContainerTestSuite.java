package org.apache.flink.connectors.test.kafka;

import org.apache.flink.connectors.test.common.environment.FlinkContainersTestEnvironment;
import org.apache.flink.connectors.test.common.environment.TestEnvironment;
import org.apache.flink.connectors.test.common.utils.FlinkJarHelper;
import org.apache.flink.connectors.test.kafka.external.KafkaContainerizedExternalSystem;

import org.junit.AfterClass;
import org.junit.BeforeClass;

/** Kafka container test suite. */
public class KafkaContainerTestSuite extends KafkaTestSuiteBase {

    public static KafkaContainerizedExternalSystem kafka;

    public static TestEnvironment testEnvironment;

    @BeforeClass
    public static void setupResources() throws Exception {
        final FlinkContainersTestEnvironment flinkContainers =
                new FlinkContainersTestEnvironment(
                        1, FlinkJarHelper.searchConnectorJar().getAbsolutePath());
        testEnvironment = flinkContainers;
        testEnvironment.startUp();
        kafka =
                new KafkaContainerizedExternalSystem()
                        .withFlinkContainers(flinkContainers.getFlinkContainers());
        kafka.startUp();
    }

    @AfterClass
    public static void tearDownResources() throws Exception {
        kafka.tearDown();
        testEnvironment.tearDown();
    }

    public KafkaContainerTestSuite(TestOptions testOptions) {
        super(testOptions);
    }

    @Override
    public String getBootstrapServer() {
        return KafkaContainerizedExternalSystem.ENTRY + "," + kafka.getBootstrapServer();
    }

    @Override
    public TestEnvironment getTestEnvironment() {
        return testEnvironment;
    }
}
