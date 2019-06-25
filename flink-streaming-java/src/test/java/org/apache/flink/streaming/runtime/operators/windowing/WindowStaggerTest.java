package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.flink.streaming.api.windowing.assigners.WindowStagger;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link WindowStagger}.
 */
public class WindowStaggerTest {
	private final long sizeInMilliseconds = 5000;

	@Test
	public void testWindowStagger() {
		assertEquals(0L, WindowStagger.ALIGNED.getStaggerOffset(500L, sizeInMilliseconds));
		assertEquals(500L, WindowStagger.NATURAL.getStaggerOffset(5500L, sizeInMilliseconds));
		assertTrue(0 < WindowStagger.RANDOM.getStaggerOffset(0L, sizeInMilliseconds));
		assertTrue(sizeInMilliseconds > WindowStagger.RANDOM.getStaggerOffset(0L, sizeInMilliseconds));
	}
}
