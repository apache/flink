package org.apache.flink.streaming.connectors.kafka.internals;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

@RunWith(Parameterized.class)
public class KafkaTopicsDescriptorTest {

	@Parameterized.Parameters
	public static Collection<Object[]> Data() {
		return Arrays.asList(new Object[][] {
			{ "topic1", null, Arrays.asList("topic1", "topic2", "topic3"), true },
			{ "topic1", null, Arrays.asList("topic2", "topic3"), false },
			{ "topic1", Pattern.compile("topic[0-9]"), null, true },
			{ "topicx", Pattern.compile("topic[0-9]"), null, false }
		});
	}

	private String topic;
	private Pattern topicPattern;
	private List<String> fixedTopics;
	boolean expected;

	public KafkaTopicsDescriptorTest(String topic, Pattern topicPattern, List<String> fixedTopics, boolean expected){
		this.topic = topic;
		this.topicPattern = topicPattern;
		this.fixedTopics = fixedTopics;
		this.expected = expected;
	}

	@Test
	public void TestIsMatchingTopic() {
		KafkaTopicsDescriptor topicsDescriptor = new KafkaTopicsDescriptor(fixedTopics, topicPattern);

		Assert.assertEquals(expected, topicsDescriptor.isMatchingTopic(topic));
	}
}
