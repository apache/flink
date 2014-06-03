/***********************************************************************************************************************
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.runtime.io.network.netty;

import eu.stratosphere.core.memory.MemorySegment;
import eu.stratosphere.nephele.event.task.AbstractEvent;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.runtime.io.Buffer;
import eu.stratosphere.runtime.io.BufferRecycler;
import eu.stratosphere.runtime.io.channels.ChannelID;
import eu.stratosphere.runtime.io.network.bufferprovider.BufferAvailabilityListener;
import eu.stratosphere.runtime.io.network.bufferprovider.BufferProvider;
import eu.stratosphere.runtime.io.network.bufferprovider.BufferProvider.BufferAvailabilityRegistration;
import eu.stratosphere.runtime.io.network.bufferprovider.BufferProviderBroker;
import eu.stratosphere.runtime.io.network.Envelope;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class InboundEnvelopeDecoderTest {

	@Mock
	private BufferProvider bufferProvider;

	@Mock
	private BufferProviderBroker bufferProviderBroker;

	@Before
	public void initMocks() throws IOException {
		MockitoAnnotations.initMocks(this);
	}

	@Test
	public void testBufferStaging() throws Exception {
		final InboundEnvelopeDecoder decoder = new InboundEnvelopeDecoder(this.bufferProviderBroker);
		final EmbeddedChannel ch = new EmbeddedChannel(
				new OutboundEnvelopeEncoder(),
				decoder);

		when(this.bufferProviderBroker.getBufferProvider(anyJobId(), anyChannelId()))
				.thenReturn(this.bufferProvider);

		// --------------------------------------------------------------------

		Envelope[] envelopes = nextEnvelopes(3, true);

		ByteBuf buf = encode(ch, envelopes);

		when(this.bufferProvider.registerBufferAvailabilityListener(Matchers.<BufferAvailabilityListener>anyObject()))
				.thenReturn(BufferAvailabilityRegistration.SUCCEEDED_REGISTERED);

		Buffer buffer = allocBuffer(envelopes[2].getBuffer().size());

		when(this.bufferProvider.requestBuffer(anyInt()))
				.thenReturn(null, null, buffer, null);

		// --------------------------------------------------------------------

		// slices: [0] => full envelope, [1] => half envelope, [2] => remaining half + full envelope
		ByteBuf[] slices = slice(buf,
				OutboundEnvelopeEncoder.HEADER_SIZE + envelopes[0].getBuffer().size(),
				OutboundEnvelopeEncoder.HEADER_SIZE + envelopes[1].getBuffer().size() / 2);

		// 1. no buffer available, incoming slice contains all data
		int refCount = slices[0].refCnt();

		decodeAndVerify(ch, slices[0]);

		Assert.assertEquals(refCount + 1, slices[0].refCnt());
		Assert.assertFalse(ch.config().isAutoRead());

		// notify of available buffer (=> bufferAvailable() callback does return a buffer
		// of the current network buffer size; the decoder needs to adjust its size to the
		// requested size
		decoder.bufferAvailable(allocBuffer(envelopes[0].getBuffer().size() * 2));
		ch.runPendingTasks();

		Assert.assertEquals(refCount - 1, slices[0].refCnt());
		Assert.assertTrue(ch.config().isAutoRead());

		decodeAndVerify(ch, envelopes[0]);

		// 2. no buffer available, incoming slice does NOT contain all data
		refCount = slices[1].refCnt();

		decodeAndVerify(ch, slices[1]);

		Assert.assertEquals(refCount + 1, slices[1].refCnt());
		Assert.assertFalse(ch.config().isAutoRead());

		decoder.bufferAvailable(allocBuffer());
		ch.runPendingTasks();

		Assert.assertEquals(refCount - 1, slices[1].refCnt());
		Assert.assertTrue(ch.config().isAutoRead());

		decodeAndVerify(ch);

		// 3. buffer available
		refCount = slices[2].refCnt();

		decodeAndVerify(ch, slices[2], envelopes[1], envelopes[2]);

		Assert.assertEquals(refCount - 1, slices[2].refCnt());
		Assert.assertTrue(ch.config().isAutoRead());

		Assert.assertEquals(1, buf.refCnt());
		buf.release();
	}

	@Test
	public void testBufferStagingStagedBufferException() throws Exception {
		final EmbeddedChannel ch = new EmbeddedChannel(
				new OutboundEnvelopeEncoder(),
				new InboundEnvelopeDecoder(this.bufferProviderBroker));

		when(this.bufferProviderBroker.getBufferProvider(anyJobId(), anyChannelId()))
				.thenReturn(this.bufferProvider);

		// --------------------------------------------------------------------

		ByteBuf buf = encode(ch, nextEnvelope(true));

		when(this.bufferProvider.requestBuffer(anyInt()))
				.thenReturn(null);

		when(this.bufferProvider.registerBufferAvailabilityListener(Matchers.<BufferAvailabilityListener>anyObject()))
				.thenReturn(BufferAvailabilityRegistration.SUCCEEDED_REGISTERED);

		// --------------------------------------------------------------------

		int refCount = buf.refCnt();

		decodeAndVerify(ch, buf);

		Assert.assertFalse(ch.config().isAutoRead());
		Assert.assertEquals(refCount + 1, buf.refCnt());

		try {
			decodeAndVerify(ch, buf);
			Assert.fail("Expected IllegalStateException not thrown");
		} catch (IllegalStateException e) {
			// expected exception
		}

		buf.release();
	}

	@Test
	public void testBufferAvailabilityRegistrationBufferAvailable() throws Exception {
		final EmbeddedChannel ch = new EmbeddedChannel(
				new OutboundEnvelopeEncoder(),
				new InboundEnvelopeDecoder(this.bufferProviderBroker));

		when(this.bufferProviderBroker.getBufferProvider(anyJobId(), anyChannelId()))
				.thenReturn(this.bufferProvider);

		// --------------------------------------------------------------------

		Envelope[] envelopes = new Envelope[]{nextEnvelope(true), nextEnvelope()};

		when(this.bufferProvider.registerBufferAvailabilityListener(Matchers.<BufferAvailabilityListener>anyObject()))
				.thenReturn(BufferAvailabilityRegistration.FAILED_BUFFER_AVAILABLE);

		when(this.bufferProvider.requestBuffer(anyInt()))
				.thenReturn(null)
				.thenReturn(allocBuffer(envelopes[0].getBuffer().size()));

		// --------------------------------------------------------------------

		ByteBuf buf = encode(ch, envelopes);

		decodeAndVerify(ch, buf, envelopes);
		Assert.assertEquals(0, buf.refCnt());
	}

	@Test
	public void testBufferAvailabilityRegistrationBufferPoolDestroyedSkipBytes() throws Exception {
		final EmbeddedChannel ch = new EmbeddedChannel(
				new OutboundEnvelopeEncoder(),
				new InboundEnvelopeDecoder(this.bufferProviderBroker));

		when(this.bufferProviderBroker.getBufferProvider(anyJobId(), anyChannelId()))
				.thenReturn(this.bufferProvider);

		when(this.bufferProvider.requestBuffer(anyInt()))
				.thenReturn(null);

		when(this.bufferProvider.registerBufferAvailabilityListener(Matchers.<BufferAvailabilityListener>anyObject()))
				.thenReturn(BufferAvailabilityRegistration.FAILED_BUFFER_POOL_DESTROYED);

		// --------------------------------------------------------------------

		Envelope[] envelopes = new Envelope[]{nextEnvelope(true), nextEnvelope(), nextEnvelope()};
		Envelope[] expectedEnvelopes = new Envelope[]{envelopes[1], envelopes[2]};

		ByteBuf buf = encode(ch, envelopes);

		int bufferSize = envelopes[0].getBuffer().size();

		// --------------------------------------------------------------------
		// 1) skip in current buffer only
		// --------------------------------------------------------------------
		{
			// skip last bytes in current buffer
			ByteBuf[] slices = slice(buf, OutboundEnvelopeEncoder.HEADER_SIZE + bufferSize);

			int refCount = slices[0].refCnt();
			decodeAndVerify(ch, slices[0]);
			Assert.assertEquals(refCount - 1, slices[0].refCnt());

			refCount = slices[1].refCnt();
			decodeAndVerify(ch, slices[1], expectedEnvelopes);
			Assert.assertEquals(refCount - 1, slices[1].refCnt());
		}

		{
			// skip bytes in current buffer, leave last 16 bytes from next envelope
			ByteBuf[] slices = slice(buf, OutboundEnvelopeEncoder.HEADER_SIZE + bufferSize + 16);

			int refCount = slices[0].refCnt();
			decodeAndVerify(ch, slices[0]);
			Assert.assertEquals(refCount - 1, slices[0].refCnt());

			refCount = slices[1].refCnt();
			decodeAndVerify(ch, slices[1], expectedEnvelopes);
			Assert.assertEquals(refCount - 1, slices[1].refCnt());
		}

		{
			// skip bytes in current buffer, then continue with full envelope from same buffer
			ByteBuf[] slices = slice(buf, OutboundEnvelopeEncoder.HEADER_SIZE + bufferSize + OutboundEnvelopeEncoder.HEADER_SIZE);

			int refCount = slices[0].refCnt();
			decodeAndVerify(ch, slices[0], expectedEnvelopes[0]);
			Assert.assertEquals(refCount - 1, slices[0].refCnt());

			refCount = slices[1].refCnt();
			decodeAndVerify(ch, slices[1], expectedEnvelopes[1]);
			Assert.assertEquals(refCount - 1, slices[1].refCnt());
		}

		// --------------------------------------------------------------------
		// 2) skip in current and next buffer
		// --------------------------------------------------------------------

		{
			// skip bytes in current buffer, then continue to skip last 32 bytes in next buffer
			ByteBuf[] slices = slice(buf, OutboundEnvelopeEncoder.HEADER_SIZE + bufferSize - 32);

			int refCount = slices[0].refCnt();
			decodeAndVerify(ch, slices[0]);
			Assert.assertEquals(refCount - 1, slices[0].refCnt());

			refCount = slices[1].refCnt();
			decodeAndVerify(ch, slices[1], expectedEnvelopes);
			Assert.assertEquals(refCount - 1, slices[1].refCnt());
		}

		{
			// skip bytes in current buffer, then continue to skip in next two buffers
			ByteBuf[] slices = slice(buf, OutboundEnvelopeEncoder.HEADER_SIZE + bufferSize - 32, 16);

			int refCount = slices[0].refCnt();
			decodeAndVerify(ch, slices[0]);
			Assert.assertEquals(refCount - 1, slices[0].refCnt());

			refCount = slices[1].refCnt();
			decodeAndVerify(ch, slices[1]);
			Assert.assertEquals(refCount - 1, slices[1].refCnt());

			refCount = slices[2].refCnt();
			decodeAndVerify(ch, slices[2], expectedEnvelopes);
			Assert.assertEquals(refCount - 1, slices[2].refCnt());
		}

		// ref count should be 1, because slices shared the ref count
		Assert.assertEquals(1, buf.refCnt());
	}

	@Test
	public void testEncodeDecode() throws Exception {
		final EmbeddedChannel ch = new EmbeddedChannel(
				new OutboundEnvelopeEncoder(), new InboundEnvelopeDecoder(this.bufferProviderBroker));

		when(this.bufferProviderBroker.getBufferProvider(anyJobId(), anyChannelId()))
				.thenReturn(this.bufferProvider);

		when(this.bufferProvider.requestBuffer(anyInt())).thenAnswer(new Answer<Object>() {
			@Override
			public Object answer(InvocationOnMock invocation) throws Throwable {
				// fulfill the buffer request
				return allocBuffer((Integer) invocation.getArguments()[0]);
			}
		});

		// --------------------------------------------------------------------

		Envelope[] envelopes = new Envelope[]{
				nextEnvelope(0),
				nextEnvelope(2),
				nextEnvelope(32768),
				nextEnvelope(3782, new TestEvent1(34872527)),
				nextEnvelope(88, new TestEvent1(8749653), new TestEvent1(365345)),
				nextEnvelope(0, new TestEvent2(34563456), new TestEvent1(598432), new TestEvent2(976293845)),
				nextEnvelope(23)
		};

		ByteBuf buf = encode(ch, envelopes);

		// 1. complete ByteBuf as input
		int refCount = buf.retain().refCnt();

		decodeAndVerify(ch, buf, envelopes);
		Assert.assertEquals(refCount - 1, buf.refCnt());

		// 2. random slices
		buf.readerIndex(0);
		ByteBuf[] slices = randomSlices(buf);

		ch.writeInbound(slices);

		for (ByteBuf slice : slices) {
			Assert.assertEquals(1, slice.refCnt());
		}

		decodeAndVerify(ch, envelopes);

		buf.release();
	}

	@Test
	public void testEncodeDecodeRandomEnvelopes() throws Exception {
		final InboundEnvelopeDecoder decoder = new InboundEnvelopeDecoder(this.bufferProviderBroker);
		final EmbeddedChannel ch = new EmbeddedChannel(
				new OutboundEnvelopeEncoder(), decoder);

		when(this.bufferProviderBroker.getBufferProvider(anyJobId(), anyChannelId()))
				.thenReturn(this.bufferProvider);

		when(this.bufferProvider.requestBuffer(anyInt())).thenAnswer(new Answer<Object>() {
			@Override
			public Object answer(InvocationOnMock invocation) throws Throwable {
				// fulfill the buffer request with the requested size
				return allocBuffer((Integer) invocation.getArguments()[0]);
			}
		});

		Random randomAnswerSource = new Random(RANDOM_SEED);

		RandomBufferRequestAnswer randomBufferRequestAnswer = new RandomBufferRequestAnswer(randomAnswerSource);

		RandomBufferAvailabilityRegistrationAnswer randomBufferAvailabilityRegistrationAnswer =
				new RandomBufferAvailabilityRegistrationAnswer(randomAnswerSource, randomBufferRequestAnswer);

		when(this.bufferProvider.requestBuffer(anyInt())).thenAnswer(randomBufferRequestAnswer);

		when(this.bufferProvider.registerBufferAvailabilityListener(Matchers.<BufferAvailabilityListener>anyObject()))
				.thenAnswer(randomBufferAvailabilityRegistrationAnswer);

		// --------------------------------------------------------------------

		Envelope[] envelopes = nextRandomEnvelopes(1024);

		ByteBuf buf = encode(ch, envelopes);

		ByteBuf[] slices = randomSlices(buf);

		for (ByteBuf slice : slices) {
			int refCount = slice.refCnt();
			ch.writeInbound(slice);

			// registered BufferAvailabilityListener => call bufferAvailable(buffer)
			while (randomBufferAvailabilityRegistrationAnswer.isRegistered()) {
				randomBufferAvailabilityRegistrationAnswer.unregister();

				Assert.assertFalse(ch.config().isAutoRead());
				Assert.assertEquals(refCount + 1, slice.refCnt());

				// return a buffer of max size => decoder needs to limit buffer size
				decoder.bufferAvailable(allocBuffer(MAX_BUFFER_SIZE));
				ch.runPendingTasks();
			}

			Assert.assertEquals(refCount - 1, slice.refCnt());
			Assert.assertTrue(ch.config().isAutoRead());
		}

		Envelope[] expected = randomBufferAvailabilityRegistrationAnswer.removeSkippedEnvelopes(envelopes);

		decodeAndVerify(ch, expected);

		Assert.assertEquals(1, buf.refCnt());

		buf.release();
	}

	// ========================================================================
	// helpers
	// ========================================================================

	private final static long RANDOM_SEED = 520346508276087l;

	private final static Random random = new Random(RANDOM_SEED);

	private final static int[] BUFFER_SIZES = new int[]{8192, 16384, 32768};

	private final static int MAX_BUFFER_SIZE = BUFFER_SIZES[2];

	private final static int MAX_NUM_EVENTS = 5;

	private final static int MAX_SLICE_SIZE = MAX_BUFFER_SIZE / 3;

	private final static int MIN_SLICE_SIZE = 1;

	private final static BufferRecycler RECYCLER = mock(BufferRecycler.class);

	// ------------------------------------------------------------------------
	// envelopes
	// ------------------------------------------------------------------------

	private static Buffer allocBuffer() {
		return allocBuffer(MAX_BUFFER_SIZE);
	}

	private static Buffer allocBuffer(int bufferSize) {
		return spy(new Buffer(new MemorySegment(new byte[bufferSize]), bufferSize, RECYCLER));
	}

	private Envelope nextEnvelope() {
		return nextEnvelope(false, false);
	}

	private Envelope nextEnvelope(boolean withBuffer) {
		return nextEnvelope(withBuffer, false);
	}

	private Envelope nextEnvelope(int bufferSize, AbstractEvent... events) {
		Envelope env = new Envelope(random.nextInt(), new JobID(), new ChannelID());
		if (bufferSize > 0) {
			byte[] data = new byte[bufferSize];
			random.nextBytes(data);

			env.setBuffer(spy(new Buffer(new MemorySegment(data), bufferSize, RECYCLER)));
		}

		if (events != null && events.length > 0) {
			env.serializeEventList(Arrays.asList(events));
		}

		return env;
	}

	private Envelope nextEnvelope(boolean withBuffer, boolean withEvents) {
		int bufferSize = 0;
		AbstractEvent[] events = null;

		if (withBuffer) {
			bufferSize = BUFFER_SIZES[random.nextInt(BUFFER_SIZES.length)];
		}

		if (withEvents) {
			events = new AbstractEvent[random.nextInt(MAX_NUM_EVENTS) + 1];

			for (int i = 0; i < events.length; i++) {
				events[i] = (random.nextBoolean()
						? new TestEvent1(random.nextLong())
						: new TestEvent2(random.nextLong()));
			}
		}

		return nextEnvelope(bufferSize, events);
	}

	private Envelope[] nextEnvelopes(int numEnvelopes, boolean withBuffer) {
		Envelope[] envelopes = new Envelope[numEnvelopes];
		for (int i = 0; i < numEnvelopes; i++) {
			envelopes[i] = nextEnvelope(withBuffer, false);
		}
		return envelopes;
	}

	private Envelope[] nextRandomEnvelopes(int numEnvelopes) {
		Envelope[] envelopes = new Envelope[numEnvelopes];
		for (int i = 0; i < numEnvelopes; i++) {
			envelopes[i] = nextEnvelope(random.nextBoolean(), random.nextBoolean());
		}
		return envelopes;
	}

	// ------------------------------------------------------------------------
	// channel encode/decode
	// ------------------------------------------------------------------------

	private static ByteBuf encode(EmbeddedChannel ch, Envelope... envelopes) {
		for (Envelope env : envelopes) {
			ch.writeOutbound(env);

			if (env.getBuffer() != null) {
				verify(env.getBuffer(), times(1)).recycleBuffer();
			}
		}

		CompositeByteBuf encodedEnvelopes = new CompositeByteBuf(ByteBufAllocator.DEFAULT, false, envelopes.length);

		ByteBuf buf;
		while ((buf = (ByteBuf) ch.readOutbound()) != null) {
			encodedEnvelopes.addComponent(buf);
		}

		return encodedEnvelopes.writerIndex(encodedEnvelopes.capacity());
	}

	private static void decodeAndVerify(EmbeddedChannel ch, ByteBuf buf, Envelope... expectedEnvelopes) {
		ch.writeInbound(buf);

		decodeAndVerify(ch, expectedEnvelopes);
	}

	private static void decodeAndVerify(EmbeddedChannel ch, Envelope... expectedEnvelopes) {
		if (expectedEnvelopes == null) {
			Assert.assertNull(ch.readInbound());
		}
		else {
			for (Envelope expected : expectedEnvelopes) {
				Envelope actual = (Envelope) ch.readInbound();

				if (actual == null) {
					Assert.fail("No inbound envelope available, but expected one");
				}

				assertEqualEnvelopes(expected, actual);
			}
		}
	}

	private static void assertEqualEnvelopes(Envelope expected, Envelope actual) {
		Assert.assertTrue(expected.getSequenceNumber() == actual.getSequenceNumber() &&
				expected.getJobID().equals(actual.getJobID()) &&
				expected.getSource().equals(actual.getSource()));

		if (expected.getBuffer() == null) {
			Assert.assertNull(actual.getBuffer());
		}
		else {
			Assert.assertNotNull(actual.getBuffer());

			ByteBuffer expectedByteBuffer = expected.getBuffer().getMemorySegment().wrap(0, expected.getBuffer().size());
			ByteBuffer actualByteBuffer = actual.getBuffer().getMemorySegment().wrap(0, actual.getBuffer().size());

			Assert.assertEquals(0, expectedByteBuffer.compareTo(actualByteBuffer));
		}

		if (expected.getEventsSerialized() == null) {
			Assert.assertNull(actual.getEventsSerialized());
		}
		else {
			Assert.assertNotNull(actual.getEventsSerialized());

			// this is needed, because the encoding of the byte buffer
			// alters the state of the buffer
			expected.getEventsSerialized().clear();

			List<? extends AbstractEvent> expectedEvents = expected.deserializeEvents();
			List<? extends AbstractEvent> actualEvents = actual.deserializeEvents();

			Assert.assertEquals(expectedEvents.size(), actualEvents.size());

			for (int i = 0; i < expectedEvents.size(); i++) {
				AbstractEvent expectedEvent = expectedEvents.get(i);
				AbstractEvent actualEvent = actualEvents.get(i);

				Assert.assertEquals(expectedEvent.getClass(), actualEvent.getClass());
				Assert.assertEquals(expectedEvent, actualEvent);
			}
		}
	}

	private static ByteBuf[] randomSlices(ByteBuf buf) {
		List<Integer> sliceSizes = new LinkedList<Integer>();

		if (buf.readableBytes() < MIN_SLICE_SIZE) {
			throw new IllegalStateException("Buffer to slice is smaller than required minimum slice size");
		}

		int available = buf.readableBytes() - MIN_SLICE_SIZE;

		while (available > 0) {
			int size = Math.min(available, Math.max(MIN_SLICE_SIZE, random.nextInt(MAX_SLICE_SIZE) + 1));
			available -= size;
			sliceSizes.add(size);
		}

		int[] slices = new int[sliceSizes.size()];
		for (int i = 0; i < sliceSizes.size(); i++) {
			slices[i] = sliceSizes.get(i);
		}

		return slice(buf, slices);
	}

	/**
	 * Returns slices with the specified sizes of the given buffer.
	 * <p/>
	 * When given n indexes, n+1 slices will be returned:
	 * <ul>
	 * <li>0 - sliceSizes[0]</li>
	 * <li>sliceSizes[0] - sliceSizes[1]</li>
	 * <li>...</li>
	 * <li>sliceSizes[n-1] - buf.capacity()</li>
	 * </ul>
	 *
	 * @return slices with the specified sizes of the given buffer
	 */
	private static ByteBuf[] slice(ByteBuf buf, int... sliceSizes) {
		if (sliceSizes.length == 0) {
			throw new IllegalStateException("Need to provide at least one slice size");
		}

		int numSlices = sliceSizes.length;
		// transform slice sizes to buffer indexes
		for (int i = 1; i < numSlices; i++) {
			sliceSizes[i] += sliceSizes[i - 1];
		}

		for (int i = 0; i < sliceSizes.length - 1; i++) {
			if (sliceSizes[i] >= sliceSizes[i + 1] || sliceSizes[i] <= 0 || sliceSizes[i] >= buf.capacity()) {
				throw new IllegalStateException(
						String.format("Slice size %s are off for %s", Arrays.toString(sliceSizes), buf));
			}
		}

		ByteBuf[] slices = new ByteBuf[numSlices + 1];

		// slice at slice indexes
		slices[0] = buf.slice(0, sliceSizes[0]).retain();
		for (int i = 1; i < numSlices; i++) {
			slices[i] = buf.slice(sliceSizes[i - 1], sliceSizes[i] - sliceSizes[i - 1]).retain();
		}
		slices[numSlices] = buf.slice(sliceSizes[numSlices - 1], buf.capacity() - sliceSizes[numSlices - 1]).retain();

		return slices;
	}

	// ------------------------------------------------------------------------
	// mocking
	// ------------------------------------------------------------------------

	private static JobID anyJobId() {
		return Matchers.anyObject();
	}

	private static ChannelID anyChannelId() {
		return Matchers.anyObject();
	}

	// these following two Answer classes are quite ugly, but they allow to implement a randomized
	// test of encoding and decoding envelopes
	private static class RandomBufferRequestAnswer implements Answer<Buffer> {

		private final Random random;

		private boolean forced;

		private RandomBufferRequestAnswer(Random random) {
			this.random = random;
		}

		@Override
		public Buffer answer(InvocationOnMock invocation) throws Throwable {
			if (this.forced) {
				Buffer toReturn = allocBuffer((Integer) invocation.getArguments()[0]);
				this.forced = false;

				return toReturn;
			}

			return this.random.nextBoolean() ? allocBuffer((Integer) invocation.getArguments()[0]) : null;
		}

		public void forceBufferAvailable() {
			this.forced = true;
		}
	}

	private static class RandomBufferAvailabilityRegistrationAnswer implements Answer<BufferAvailabilityRegistration> {

		private final Random random;

		private final RandomBufferRequestAnswer bufferRequestAnswer;

		private boolean isRegistered = false;

		private int numSkipped;

		private RandomBufferAvailabilityRegistrationAnswer(Random random, RandomBufferRequestAnswer bufferRequestAnswer) {
			this.random = random;
			this.bufferRequestAnswer = bufferRequestAnswer;
		}

		@Override
		public BufferAvailabilityRegistration answer(InvocationOnMock invocation) throws Throwable {
			if (this.random.nextBoolean()) {
				this.isRegistered = true;
				return BufferAvailabilityRegistration.SUCCEEDED_REGISTERED;
			}
			else if (this.random.nextBoolean()) {
				this.bufferRequestAnswer.forceBufferAvailable();
				return BufferAvailabilityRegistration.FAILED_BUFFER_AVAILABLE;
			}
			else {
				this.numSkipped++;
				return BufferAvailabilityRegistration.FAILED_BUFFER_POOL_DESTROYED;
			}
		}

		public Envelope[] removeSkippedEnvelopes(Envelope[] envelopes) {
			this.random.setSeed(RANDOM_SEED);
			Envelope[] envelopesWithoutSkipped = new Envelope[envelopes.length - this.numSkipped];
			int numEnvelopes = 0;

			for (Envelope env : envelopes) {
				if (env.getBuffer() != null) {
					// skip envelope if returned FAILED_BUFFER_POOL_DESTROYED
					if (!this.random.nextBoolean() && !this.random.nextBoolean() && !this.random.nextBoolean()) {
						continue;
					}
				}

				envelopesWithoutSkipped[numEnvelopes++] = env;
			}

			return envelopesWithoutSkipped;
		}

		public boolean isRegistered() {
			return this.isRegistered;
		}

		public void unregister() {
			this.isRegistered = false;
		}
	}

	// ------------------------------------------------------------------------

	public static final class TestEvent1 extends AbstractEvent {

		private long id;

		public TestEvent1() {
		}

		public TestEvent1(long id) {
			this.id = id;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeLong(id);
		}

		@Override
		public void read(DataInput in) throws IOException {
			id = in.readLong();
		}


		@Override
		public boolean equals(Object obj) {
			return obj.getClass() == TestEvent1.class && ((TestEvent1) obj).id == this.id;
		}

		@Override
		public int hashCode() {
			return ((int) id) ^ ((int) (id >>> 32));
		}

		@Override
		public String toString() {
			return "TestEvent1 (" + id + ")";
		}
	}

	public static final class TestEvent2 extends AbstractEvent {

		private long id;

		public TestEvent2() {
		}

		public TestEvent2(long id) {
			this.id = id;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeLong(id);
		}

		@Override
		public void read(DataInput in) throws IOException {
			id = in.readLong();
		}

		@Override
		public boolean equals(Object obj) {
			return obj.getClass() == TestEvent2.class && ((TestEvent2) obj).id == this.id;
		}

		@Override
		public int hashCode() {
			return ((int) id) ^ ((int) (id >>> 32));
		}

		@Override
		public String toString() {
			return "TestEvent2 (" + id + ")";
		}
	}
}
