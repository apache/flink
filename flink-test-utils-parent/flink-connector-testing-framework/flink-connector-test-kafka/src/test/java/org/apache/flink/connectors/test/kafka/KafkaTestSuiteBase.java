package org.apache.flink.connectors.test.kafka;

import org.apache.flink.connectors.test.common.external.ExternalContext;
import org.apache.flink.connectors.test.common.testsuites.TestSuiteBase;
import org.apache.flink.connectors.test.kafka.external.KafkaMultipleTopicExternalContext;
import org.apache.flink.connectors.test.kafka.external.KafkaSingleTopicExternalContext;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

/** Suite. */
@RunWith(Parameterized.class)
public abstract class KafkaTestSuiteBase extends TestSuiteBase<String> {

    private ExternalContext<String> externalContext;

    @Parameterized.Parameters(name = "{index}: {0}")
    public static Collection<Object[]> parameters() {
        return Arrays.stream(TestOptions.values())
                .map(option -> new Object[] {option})
                .collect(Collectors.toList());
    }

    enum TestOptions {
        PER_PARTITION_SPLIT,
        PER_TOPIC_SPLIT
    }

    public KafkaTestSuiteBase(TestOptions testOptions) {
        switch (testOptions) {
            case PER_PARTITION_SPLIT:
                {
                    this.externalContext =
                            new KafkaSingleTopicExternalContext(getBootstrapServer());
                    break;
                }
            case PER_TOPIC_SPLIT:
                {
                    this.externalContext =
                            new KafkaMultipleTopicExternalContext(getBootstrapServer());
                }
        }
    }

    @Override
    public ExternalContext<String> getExternalContext() {
        return externalContext;
    }

    public abstract String getBootstrapServer();
}
