package org.apache.flink.streaming.connectors.fs;

import org.junit.Test;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;

// import static junit.framework.TestCase.assertTrue;

/**
 * Tests for {@link CompressionStringWriter}.
 */
public class CompressionStringWriterTest {

	@Test
	public void testDuplicate() {
		CompressionStringWriter<String> writer = new CompressionStringWriter<>("Gzip", "\n");
		writer.setSyncOnFlush(true);
		CompressionStringWriter<String> other = writer.duplicate();

		assertTrue(StreamWriterBaseComparator.equals(writer, other));
		writer.setSyncOnFlush(false);
		assertFalse(StreamWriterBaseComparator.equals(writer, other));
	}

}
