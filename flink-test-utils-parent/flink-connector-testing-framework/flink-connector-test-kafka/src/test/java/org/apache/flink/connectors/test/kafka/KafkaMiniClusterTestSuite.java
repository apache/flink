package org.apache.flink.connectors.test.kafka;

import org.apache.flink.connectors.test.common.environment.MiniClusterTestEnvironment;
import org.apache.flink.connectors.test.common.environment.TestEnvironment;
import org.apache.flink.connectors.test.kafka.external.KafkaContainerizedExternalSystem;

import org.junit.AfterClass;
import org.junit.BeforeClass;

/** Kafka mini cluster test suite. */
public class KafkaMiniClusterTestSuite extends KafkaTestSuiteBase {

    public static KafkaContainerizedExternalSystem kafka;

    public static MiniClusterTestEnvironment miniClusterTestEnvironment;

    @BeforeClass
    public static void setupResources() throws Exception {
        miniClusterTestEnvironment = new MiniClusterTestEnvironment();
        miniClusterTestEnvironment.startUp();
        kafka = new KafkaContainerizedExternalSystem();
        kafka.startUp();
    }

    @AfterClass
    public static void tearDownResources() throws Exception {
        kafka.tearDown();
        miniClusterTestEnvironment.tearDown();
    }

    public KafkaMiniClusterTestSuite(TestOptions testOptions) {
        super(testOptions);
    }

    @Override
    public String getBootstrapServer() {
        return kafka.getBootstrapServer();
    }

    @Override
    public TestEnvironment getTestEnvironment() {
        return miniClusterTestEnvironment;
    }
}
