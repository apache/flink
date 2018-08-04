package org.apache.flink.streaming.connectors.fs.bucketing;

import org.apache.flink.streaming.connectors.fs.Clock;

import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.TimeZone;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * Tests for {@link DateTimeBucketer}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(DateTimeBucketer.class)
public class DateTimeBucketerTest {
	private static final long TEST_TIME_IN_MILLIS = 1533363082011L;
	private static final Path TEST_PATH = new Path("test");

	@Test
	public void testGetBucketPathWithDefaultTimezone() {
		TimeZone utc = TimeZone.getTimeZone("UTC");
		PowerMockito.mockStatic(TimeZone.class);
		when(TimeZone.getDefault()).thenReturn(utc);

		DateTimeBucketer bucketer = new DateTimeBucketer();

		Clock clock = mock(Clock.class);
		when(clock.currentTimeMillis()).thenReturn(TEST_TIME_IN_MILLIS);

		assertEquals(new Path("test/2018-08-04--06"), bucketer.getBucketPath(clock, TEST_PATH, null));
	}

	@Test
	public void testGetBucketPathWithSpecifiedTimezone() {
		Clock clock = mock(Clock.class);
		when(clock.currentTimeMillis()).thenReturn(TEST_TIME_IN_MILLIS);
		DateTimeBucketer bucketer = new DateTimeBucketer(TimeZone.getTimeZone("PST"));

		assertEquals(new Path("test/2018-08-03--23"), bucketer.getBucketPath(clock, TEST_PATH, null));
	}
}
