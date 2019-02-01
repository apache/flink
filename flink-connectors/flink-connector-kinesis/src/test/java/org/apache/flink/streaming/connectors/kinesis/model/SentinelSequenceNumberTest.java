package org.apache.flink.streaming.connectors.kinesis.model;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class SentinelSequenceNumberTest {

	@Test
	public void allSentinelNumbersAreRecognized() {
		for (SentinelSequenceNumber sentinel : SentinelSequenceNumber.values()) {
			assertTrue(SentinelSequenceNumber.isSentinelSequenceNumber(sentinel.get()));
		}
	}
}
