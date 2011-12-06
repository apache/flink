/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.io.channels.bytebuffered;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.EOFException;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import eu.stratosphere.nephele.io.DefaultRecordDeserializer;
import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.channels.AbstractChannel;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.DeserializationBuffer;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.io.compression.CompressionLoader;
import eu.stratosphere.nephele.io.compression.Decompressor;
import eu.stratosphere.nephele.types.StringRecord;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * This class check the functionality of {@link FileInputChannel} class
 * and thereby of the {@link AbstractByteBufferedInputChannel} and {@link AbstractChannel} class.
 * 
 * @author marrus
 */
@RunWith(PowerMockRunner.class)
@SuppressStaticInitializationFor("eu.stratosphere.nephele.io.channels.AbstractChannel")
public class FileInputChannelTest {
	@Mock
	private Buffer uncompressedDataBuffer;

	@Mock
	DeserializationBuffer<StringRecord> deserializationBuffer;

	@Mock
	ChannelID id;

	@Mock
	ChannelID connected;

	@Mock
	DefaultRecordDeserializer<StringRecord> deserializer;

	/**
	 * Set up mocks
	 * 
	 * @throws IOException
	 */
	@Before
	public void before() throws Exception {
		MockitoAnnotations.initMocks(this);
	}

	/**
	 * This test checks the functionality of the deserializeNextRecod() method
	 * 
	 * @throws IOException
	 */
	@Test
	@PrepareForTest(CompressionLoader.class)
	public void deserializeNextRecordTest() throws IOException, InterruptedException {
		StringRecord record = new StringRecord("abc");
		Decompressor decompressorMock = mock(Decompressor.class);
		this.uncompressedDataBuffer = mock(Buffer.class);
		BufferPairResponse bufferPair = new BufferPairResponse(this.uncompressedDataBuffer, this.uncompressedDataBuffer);
		// BufferPairResponse bufferPair = mock(BufferPairResponse.class);
		// when(bufferPair.getUncompressedDataBuffer()).thenReturn(this.uncompressedDataBuffer,
		// this.uncompressedDataBuffer, null);

		PowerMockito.mockStatic(CompressionLoader.class);
		when(
			CompressionLoader.getDecompressorByCompressionLevel(Matchers.any(CompressionLevel.class),
				Matchers.any(FileInputChannel.class))).thenReturn(
			decompressorMock);

		@SuppressWarnings("unchecked")
		final InputGate<StringRecord> inGate = mock(InputGate.class);
		final ByteBufferedInputChannelBroker inputBroker = mock(ByteBufferedInputChannelBroker.class);
		when(inputBroker.getReadBufferToConsume()).thenReturn(bufferPair);
		try {
			when(
				this.deserializationBuffer.readData(Matchers.any(StringRecord.class),
					Matchers.any(ReadableByteChannel.class))).thenReturn(null, record);
		} catch (IOException e) {

		}
		when(this.uncompressedDataBuffer.remaining()).thenReturn(0);

		// setup test-object
		final FileInputChannel<StringRecord> fileInputChannel = new FileInputChannel<StringRecord>(inGate, 1,
			this.deserializer,
			null, CompressionLevel.NO_COMPRESSION);
		fileInputChannel.setInputChannelBroker(inputBroker);

		Whitebox.setInternalState(fileInputChannel, "deserializationBuffer", this.deserializationBuffer);

		// correct run
		try {
			fileInputChannel.readRecord(null);
		} catch (IOException e) {
			fail(StringUtils.stringifyException(e));
		}

		// Close Channel to test EOFException
		try {
			fileInputChannel.close();
		} catch (IOException e) {
			fail(StringUtils.stringifyException(e));
		} catch (InterruptedException e) {
			fail(StringUtils.stringifyException(e));
		}
		// No acknowledgment from consumer yet so the channel should still be open
		assertEquals(false, fileInputChannel.isClosed());
		fileInputChannel.processEvent(new ByteBufferedChannelCloseEvent());
		// Received acknowledgment the channel should be closed now
		assertEquals(true, fileInputChannel.isClosed());
		try {
			fileInputChannel.readRecord(null);
			fail();
		} catch (EOFException e) {
			// expected a EOFException
		} catch (IOException e) {
			// all other Exceptions are real failures
			e.printStackTrace();
			fail();
		}
	}

}
