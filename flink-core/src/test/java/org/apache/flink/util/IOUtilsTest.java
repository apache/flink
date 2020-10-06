package org.apache.flink.util;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;

/**
 * Tests for the {@link IOUtils}.
 */
public class IOUtilsTest extends TestLogger {

	@Test
	public void testReadFullyFromLongerStream() throws IOException {
		ByteArrayInputStream inputStream = new ByteArrayInputStream("test-data".getBytes());

		byte[] out = new byte[4];
		int read = IOUtils.readFully(inputStream, out);

		Assert.assertArrayEquals("test".getBytes(), Arrays.copyOfRange(out, 0, read));
	}

	@Test
	public void testReadFullyFromShorterStream() throws IOException {
		ByteArrayInputStream inputStream = new ByteArrayInputStream("t".getBytes());

		byte[] out = new byte[4];
		int read = IOUtils.readFully(inputStream, out);

		Assert.assertArrayEquals("t".getBytes(), Arrays.copyOfRange(out, 0, read));
	}
}
