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

import java.io.IOException;

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
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.channels.AbstractChannel;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.SerializationBuffer;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.io.compression.CompressionLoader;
import eu.stratosphere.nephele.io.compression.Decompressor;
import eu.stratosphere.nephele.types.StringRecord;

/**
 * This class check the functionality of {@link FileInputChannel} class 
 * and thereby of the {@link AbstractByteBufferedInputChannel} and {@link AbstractChannel} class.
 * @author marrus
 *
 */
@RunWith(PowerMockRunner.class)
@SuppressStaticInitializationFor("eu.stratosphere.nephele.io.channels.AbstractChannel")
public class FileOutputChannelTest {
	@Mock
	private Buffer uncompressedDataBuffer;

	@Mock
	SerializationBuffer<StringRecord> serializationBuffer;
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
	 * @throws IOException
	 * @throws InterruptedException 
	 */
	@Test
	@PrepareForTest(CompressionLoader.class)
	public void writeRecordTest() throws IOException, InterruptedException {
		
		final StringRecord record = new StringRecord("abc");
		final Decompressor decompressorMock = mock(Decompressor.class);
		this.uncompressedDataBuffer = mock(Buffer.class);
		BufferPairResponse bufferPair = new BufferPairResponse(this.uncompressedDataBuffer, this.uncompressedDataBuffer);
		//BufferPairResponse bufferPair = mock(BufferPairResponse.class);
		//when(bufferPair.getUncompressedDataBuffer()).thenReturn(this.uncompressedDataBuffer, this.uncompressedDataBuffer, this.uncompressedDataBuffer,null);
		//when(bufferPair.getCompressedDataBuffer()).thenReturn(this.uncompressedDataBuffer, this.uncompressedDataBuffer, this.uncompressedDataBuffer,null);
		PowerMockito.mockStatic(CompressionLoader.class);
		when(CompressionLoader.getDecompressorByCompressionLevel(Matchers.any(CompressionLevel.class))).thenReturn(
			decompressorMock);
		
		@SuppressWarnings("unchecked")
		final OutputGate<StringRecord> outGate = mock(OutputGate.class);
		final ByteBufferedOutputChannelBroker outputBroker = mock(ByteBufferedOutputChannelBroker.class);
		when(outputBroker.requestEmptyWriteBuffers()).thenReturn(bufferPair);
		
		when(this.serializationBuffer.dataLeftFromPreviousSerialization()).thenReturn(true,false,false,true,false);
//		try {
//			when(this.serializationBuffer.readData(Matchers.any(ReadableByteChannel.class))).thenReturn(null, record);
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
		when(this.uncompressedDataBuffer.remaining()).thenReturn(0);
		
		//setup test-object
		FileOutputChannel<StringRecord> fileOutputChannel = new FileOutputChannel<StringRecord>(outGate, 1, null, CompressionLevel.NO_COMPRESSION);
		fileOutputChannel.setByteBufferedOutputChannelBroker(outputBroker);

		Whitebox.setInternalState(fileOutputChannel, "serializationBuffer",this.serializationBuffer);
	

		//correct run
		try {
			fileOutputChannel.writeRecord(record);
		} catch (IOException e) {
			fail();
			e.printStackTrace();
		}

		// Close Channel to test EOFException
		fileOutputChannel.requestClose();
		//No acknowledgment from consumer yet so the channel should still be open
		assertEquals(false, fileOutputChannel.isClosed());
		fileOutputChannel.processEvent(new ByteBufferedChannelCloseEvent());
		//Received acknowledgment the channel should be closed now
		assertEquals(true, fileOutputChannel.isClosed());
		try {
			fileOutputChannel.writeRecord(record);
			fail();
		} catch (IOException e) {
			// expected a IOException
		} 
	}

}
