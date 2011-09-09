package eu.stratosphere.pact.common.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.powermock.api.mockito.PowerMockito.when;

import java.io.EOFException;
import java.io.IOException;
import java.io.StringBufferInputStream;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.fs.hdfs.DistributedDataInputStream;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactString;

@SuppressWarnings("deprecation")
@RunWith(PowerMockRunner.class)
@PrepareForTest(DistributedDataInputStream.class)
public class TextInputFormatTest {

	private static final String myString = "my mocked line 1\nmy mocked line 2\n";

	class MyTextInputFormat extends TextInputFormat<PactString, PactString> {

		@Override
		public boolean readLine(KeyValuePair<PactString, PactString> pair, byte[] record) {
			String theRecord = new String(record);
			pair.getKey().setValue(theRecord.substring(0, theRecord.indexOf('|')));
			pair.getValue().setValue(theRecord.substring(theRecord.indexOf('|') + 1));
			return true;
		}
	}

	class MyStringBufferIS extends StringBufferInputStream implements Seekable, PositionedReadable {

		public MyStringBufferIS(String s) {
			super(s);
		}

		@Override
		public int read(long position, byte[] buffer, int offset, int length) throws IOException {
			return this.read(buffer, offset, length);
		}

		@Override
		public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
			int bytesRead = this.read(buffer, offset, length);
			if (bytesRead != length) {
				throw new EOFException("more bytes to read than available!");
			}
		}

		@Override
		public void readFully(long position, byte[] buffer) throws IOException {
			this.read(buffer, 0, buffer.length);
		}

		@Override
		public void seek(long pos) throws IOException {
			// ignore this
		}

		@Override
		public long getPos() throws IOException {
			return 0;
		}

		@Override
		public boolean seekToNewSource(long targetPos) throws IOException {
			// not implemented
			return false;
		}

	}

	MyStringBufferIS is = new MyStringBufferIS(myString);

	@Mock
	Configuration config;

	@Mock
	DistributedDataInputStream mockedStream;

	private TextInputFormat<PactString, PactString> format = new MyTextInputFormat();

	@Before
	public void setup() {
		initMocks(this);
	}

	@Test
	public void testInitTypes() {
		this.format.initTypes();
		assertEquals(PactString.class, format.getOutKeyType());
		assertEquals(PactString.class, format.getOutValueType());
	}

	@Test
	public void testConfigure() {
		when(this.config.getString(Matchers.matches(TextInputFormat.FORMAT_PAIR_DELIMITER), Matchers.anyString()))
			.thenReturn("\n");
		format.configure(this.config);
		verify(this.config, times(1)).getString(Matchers.any(String.class), Matchers.any(String.class));
		assertEquals("\n", new String(format.getDelimiter()));

		when(this.config.getString(Matchers.matches(TextInputFormat.FORMAT_PAIR_DELIMITER), Matchers.anyString()))
			.thenReturn("&-&");
		format.configure(this.config);
		verify(this.config, times(2)).getString(Matchers.any(String.class), Matchers.any(String.class));
		assertEquals("&-&", new String(format.getDelimiter()));
	}

	@Test
	public void testOpen() throws IOException {
		// no in-depth test possible, must be done by an integration test
		int bufferSize = 5;
		format.setInput(new DistributedDataInputStream(new FSDataInputStream(is)), 0l, myString.length(), bufferSize);
		format.open();
		assertEquals(0, format.start);
		assertEquals(myString.length() - bufferSize, format.length);
		assertEquals(bufferSize, format.bufferSize);
	}

	@Test
	public void testCreatePair() {
		KeyValuePair<PactString, PactString> pair = format.createPair();
		assertNotNull(pair);
		assertNotNull(pair.getKey());
		assertNotNull(pair.getValue());

	}

	@Test
	public void testRead() throws IOException {
		String myString = "my key|my val$$$my key2\n$$ctd.$$|my value2";
		MyStringBufferIS is = new MyStringBufferIS(myString);
		Configuration parameters = new Configuration();
		parameters.setString(TextInputFormat.FORMAT_PAIR_DELIMITER, "$$$");
		format.configure(parameters);
		format.setInput(new DistributedDataInputStream(new FSDataInputStream(is)), 0, myString.length(), 5);
		format.open();
		KeyValuePair<PactString, PactString> pair = new KeyValuePair<PactString, PactString>();
		pair.setKey(new PactString());
		pair.setValue(new PactString());
		assertTrue(format.nextPair(pair));
		assertEquals("my key", pair.getKey().toString());
		assertEquals("my val", pair.getValue().toString());
		assertTrue(format.nextPair(pair));
		assertEquals("my key2\n$$ctd.$$", pair.getKey().getValue());
		assertEquals("my value2", pair.getValue().toString());
		assertFalse(format.nextPair(pair));
		assertTrue(format.reachedEnd());

		// 2. test case
		myString = "my key|my val$$$my key2\n$$ctd.$$|my value2";
		is = new MyStringBufferIS(myString);
		parameters = new Configuration();
		parameters.setString(TextInputFormat.FORMAT_PAIR_DELIMITER, "\n");
		format.configure(parameters);
		format.setInput(new DistributedDataInputStream(new FSDataInputStream(is)), 0, myString.length(), 5);
		format.open();
		pair = new KeyValuePair<PactString, PactString>();
		pair.setKey(new PactString());
		pair.setValue(new PactString());
		assertTrue(format.nextPair(pair));
		assertEquals("my key", pair.getKey().toString());
		assertEquals("my val$$$my key2", pair.getValue().toString());
		assertTrue(format.nextPair(pair));
		assertEquals("$$ctd.$$", pair.getKey().getValue());
		assertEquals("my value2", pair.getValue().toString());
		assertFalse(format.nextPair(pair));
		assertTrue(format.reachedEnd());

	}

}
