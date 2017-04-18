package org.apache.flink.metrics.datadog;

import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DatadogHttpReporterTests {
	private static DatadogHttpReporter reporter;

	@BeforeClass
	public static void init() {
		reporter = new DatadogHttpReporter();
	}

	@Test
	public void testFilterChars() {
		assertEquals("", reporter.filterCharacters(""));
        assertEquals("abc", reporter.filterCharacters("abc"));
        assertEquals("a.b..", reporter.filterCharacters("a.b.."));
	}
}
