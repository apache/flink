package org.apache.flink.metrics.datadog;

import org.junit.Test;

public class DatadogHttpClientTests {
	@Test(expected = IllegalArgumentException.class)
	public void testValidateApiKey() {
		new DatadogHttpClient("fake_key");
	}
}
