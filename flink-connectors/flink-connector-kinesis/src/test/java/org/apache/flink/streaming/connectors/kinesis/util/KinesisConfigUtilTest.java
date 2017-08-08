package org.apache.flink.streaming.connectors.kinesis.util;

import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.ProducerConfigConstants;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Properties;

import static org.junit.Assert.assertEquals;

/**
 * Tests for KinesisConfigUtil.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FlinkKinesisConsumer.class, KinesisConfigUtil.class})
public class KinesisConfigUtilTest {
	@Rule
	private ExpectedException exception = ExpectedException.none();

	@Test
	public void testUnparsableLongForProducerConfiguration() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Error trying to set field RateLimit with the value 'unparsableLong'");

		Properties testConfig = new Properties();
		testConfig.setProperty(ProducerConfigConstants.AWS_REGION, "us-east-1");
		testConfig.setProperty("RateLimit", "unparsableLong");

		KinesisConfigUtil.validateProducerConfiguration(testConfig);
	}

	@Test
	public void testReplaceDeprecatedKeys() {
		Properties testConfig = new Properties();
		testConfig.setProperty(ProducerConfigConstants.AWS_REGION, "us-east-1");
		testConfig.setProperty(ProducerConfigConstants.DEPRECATED_AGGREGATION_MAX_COUNT, "1");
		testConfig.setProperty(ProducerConfigConstants.DEPRECATED_COLLECTION_MAX_COUNT, "2");
		Properties replacedConfig = KinesisConfigUtil.replaceDeprecatedProducerKeys(testConfig);

		assertEquals("1", replacedConfig.getProperty(ProducerConfigConstants.AGGREGATION_MAX_COUNT));
		assertEquals("2", replacedConfig.getProperty(ProducerConfigConstants.COLLECTION_MAX_COUNT));
	}
}
