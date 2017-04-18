package org.apache.flink.metrics.datadog.parser;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.flink.metrics.MetricConfig;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MetricParserTests {
	private static MetricParser parserWithTags;
	private static MetricParser parserWithoutTags;

	private static final String TEST_TAG = "test_tags";

	@BeforeClass
	public static void init() {
		MetricConfig configWithTags = mock(MetricConfig.class);
		when(configWithTags.getBoolean(MetricParser.TAGS_ENABLED, false)).thenReturn(true);
		when(configWithTags.getString(MetricParser.TAGS, null)).thenReturn(TEST_TAG);
		parserWithTags = new MetricParser(configWithTags);

		MetricConfig configWithoutTags = mock(MetricConfig.class);
		when(configWithoutTags.getBoolean(MetricParser.TAGS_ENABLED, false)).thenReturn(false);
		parserWithoutTags = new MetricParser(configWithoutTags);
	}

	@Test
	public void testJobManagerMetric() {
		String host = "localhost";
		String metricName = "jobmanager.Status.JVM.CPU.Time";

		String fullMetricName = Joiner.on(".").join(host, metricName);

		// When tags enabled
		NameAndTags nat = parserWithTags.getNameAndTags(fullMetricName);
		assertEquals(metricName, nat.getName());
		assertEquals(Lists.newArrayList(host, TEST_TAG), nat.getTags());

		// When tags disabled
		nat = parserWithoutTags.getNameAndTags(fullMetricName);
		assertEquals(fullMetricName, nat.getName());
		assertEquals(Lists.newArrayList(), nat.getTags());
	}

	@Test
	public void testTaskManagerMetricWhenEnabled() {
		String host = "localhost";
		String tmId = "0cdc0917d4e77939edca94edcae0c2c2";
		String metricName = "taskmanager.Status.JVM.Memory.Direct.Count";
		NameAndTags nat = parserWithTags.getNameAndTags(Joiner.on(".").join(host, tmId, metricName));
		assertEquals(metricName, nat.getName());
		assertEquals(Lists.newArrayList(host, tmId, TEST_TAG), nat.getTags());
	}

	@Test
	public void testTaskMetric() {
		String host = "localhost";
		String tmId = "0cdc0917d4e77939edca94edcae0c2c2";
		String jobName = "WordCount Example";
		String subtaskIndex = "0";
		String taskName = "CHAIN DataSource (at getDefaultTextLineDataSet(WordCountData.java:70) (org.apache.flink.api.java.io.CollectionInputFormat)) -> FlatMap (FlatMap at main(WordCount.java:83)) -> Combine(WordCount.java:84)]";
		String metricName = "task.buffers.inputQueueLength";
		NameAndTags nat = parserWithTags.getNameAndTags(Joiner.on(".").join(host, tmId, jobName, subtaskIndex, taskName, metricName));
		assertEquals(metricName, nat.getName());
		assertEquals(Lists.newArrayList(host, tmId, jobName, subtaskIndex, taskName, TEST_TAG), nat.getTags());
	}

	@Test
	public void testOperatorMetricWhenEnabled() {
		String host = "localhost";
		String tmId = "0cdc0917d4e77939edca94edcae0c2c2";
		String jobName = "WordCount Example";
		String subtaskIndex = "0";
		String operatorName = "DataSource (at getDefaultTextLineDataSet(WordC ountData.java:70) (org.apache.flink.api.java.io.CollectionInputFormat))";
		String metricName = "operator.numRecordsIn";

		NameAndTags nat = parserWithTags.getNameAndTags(Joiner.on(".").join(host, tmId, jobName, subtaskIndex, operatorName, metricName));

		assertEquals(metricName, nat.getName());
		assertEquals(Lists.newArrayList(host, tmId, jobName, subtaskIndex, operatorName, TEST_TAG), nat.getTags());
	}
}
