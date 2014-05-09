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

import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.runtime.io.Buffer;
import eu.stratosphere.runtime.io.channels.ChannelID;
import eu.stratosphere.runtime.io.network.bufferprovider.BufferAvailabilityListener;
import eu.stratosphere.runtime.io.network.bufferprovider.BufferProvider;
import eu.stratosphere.runtime.io.network.bufferprovider.BufferProviderBroker;
import eu.stratosphere.runtime.io.network.Envelope;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;

public class InboundEnvelopeDecoder extends ChannelInboundHandlerAdapter implements BufferAvailabilityListener {

	private final BufferProviderBroker bufferProviderBroker;

	private final BufferAvailabilityChangedTask bufferAvailabilityChangedTask = new BufferAvailabilityChangedTask();

	private final ConcurrentLinkedQueue<Buffer> bufferBroker = new ConcurrentLinkedQueue<Buffer>();

	private final ByteBuffer headerBuffer;

	private Envelope currentEnvelope;

	private ByteBuffer currentEventsBuffer;

	private ByteBuffer currentDataBuffer;

	private int currentBufferRequestSize;

	private BufferProvider currentBufferProvider;

	private JobID lastJobId;

	private ChannelID lastSourceId;

	private ByteBuf stagedBuffer;

	private ChannelHandlerContext channelHandlerContext;

	private int bytesToSkip;

	private enum DecoderState {
		COMPLETE,
		PENDING,
		NO_BUFFER_AVAILABLE
	}

	public InboundEnvelopeDecoder(BufferProviderBroker bufferProviderBroker) {
		this.bufferProviderBroker = bufferProviderBroker;
		this.headerBuffer = ByteBuffer.allocateDirect(OutboundEnvelopeEncoder.HEADER_SIZE);
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		if (this.channelHandlerContext == null) {
			this.channelHandlerContext = ctx;
		}

		super.channelActive(ctx);
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		if (this.stagedBuffer != null) {
			throw new IllegalStateException("No channel read event should be fired " +
					"as long as the a buffer is staged.");
		}

		ByteBuf in = (ByteBuf) msg;

		if (this.bytesToSkip > 0) {
			this.bytesToSkip = skipBytes(in, this.bytesToSkip);

			// we skipped over the whole buffer
			if (this.bytesToSkip > 0) {
				in.release();
				return;
			}
		}

		decodeBuffer(in, ctx);
	}

	/**
	 * Decodes all Envelopes contained in a Netty ByteBuf and forwards them in the pipeline.
	 * Returns true and releases the buffer, if it was fully consumed. Otherwise, returns false and retains the buffer.
	 * </p>
	 * In case of no buffer availability (returns false), a buffer availability listener is registered and the input
	 * buffer is staged for later consumption.
	 *
	 * @return <code>true</code>, if buffer fully consumed, <code>false</code> otherwise
	 * @throws IOException
	 */
	private boolean decodeBuffer(ByteBuf in, ChannelHandlerContext ctx) throws IOException {

		DecoderState decoderState;
		while ((decoderState = decodeEnvelope(in)) != DecoderState.PENDING) {
			if (decoderState == DecoderState.COMPLETE) {
				ctx.fireChannelRead(this.currentEnvelope);
				this.currentEnvelope = null;
			}
			else if (decoderState == DecoderState.NO_BUFFER_AVAILABLE) {
				switch (this.currentBufferProvider.registerBufferAvailabilityListener(this)) {
					case REGISTERED:
						if (ctx.channel().config().isAutoRead()) {
							ctx.channel().config().setAutoRead(false);
						}

						this.stagedBuffer = in;
						this.stagedBuffer.retain();
						return false;

					case NOT_REGISTERED_BUFFER_AVAILABLE:
						continue;

					case NOT_REGISTERED_BUFFER_POOL_DESTROYED:
						this.bytesToSkip = skipBytes(in, this.currentBufferRequestSize);

						this.currentBufferRequestSize = 0;
						this.currentEventsBuffer = null;
						this.currentEnvelope = null;
				}
			}
		}

		if (in.isReadable()) {
			throw new IllegalStateException("Every buffer should have been fully" +
					"consumed after *successfully* decoding it (if it was not successful, " +
					"the buffer will be staged for later consumption).");
		}

		in.release();
		return true;
	}

	/**
	 * Notifies the IO thread that a Buffer has become available again.
	 * <p/>
	 * This method will be called from outside the Netty IO thread. The caller will be the buffer pool from which the
	 * available buffer comes (i.e. the InputGate).
	 * <p/>
	 * We have to make sure that the available buffer is handed over to the IO thread in a safe manner.
	 */
	@Override
	public void bufferAvailable(Buffer buffer) throws Exception {
		this.bufferBroker.offer(buffer);
		this.channelHandlerContext.channel().eventLoop().execute(this.bufferAvailabilityChangedTask);
	}

	/**
	 * Continues the decoding of a staged buffer after a buffer has become available again.
	 * <p/>
	 * This task should be executed by the IO thread to ensure safe access to the staged buffer.
	 */
	private class BufferAvailabilityChangedTask implements Runnable {
		@Override
		public void run() {
			Buffer availableBuffer = bufferBroker.poll();
			if (availableBuffer == null) {
				throw new IllegalStateException("The BufferAvailabilityChangedTask" +
						"should only be executed when a Buffer has been offered" +
						"to the Buffer broker (after becoming available).");
			}

			// This alters the state of the last `decodeEnvelope(ByteBuf)`
			// call to set the buffer, which has become available again
			availableBuffer.limitSize(currentBufferRequestSize);
			currentEnvelope.setBuffer(availableBuffer);
			currentDataBuffer = availableBuffer.getMemorySegment().wrap(0, InboundEnvelopeDecoder.this.currentBufferRequestSize);
			currentBufferRequestSize = 0;

			stagedBuffer.release();

			try {
				if (decodeBuffer(stagedBuffer, channelHandlerContext)) {
					stagedBuffer = null;
					channelHandlerContext.channel().config().setAutoRead(true);
				}
			} catch (IOException e) {
				availableBuffer.recycleBuffer();
			}
		}
	}

	// --------------------------------------------------------------------

	private DecoderState decodeEnvelope(ByteBuf in) throws IOException {
		// --------------------------------------------------------------------
		// (1) header (EnvelopeEncoder.HEADER_SIZE bytes)
		// --------------------------------------------------------------------
		if (this.currentEnvelope == null) {
			copy(in, this.headerBuffer);

			if (this.headerBuffer.hasRemaining()) {
				return DecoderState.PENDING;
			}
			else {
				this.headerBuffer.flip();

				int magicNum = this.headerBuffer.getInt();
				if (magicNum != OutboundEnvelopeEncoder.MAGIC_NUMBER) {
					throw new IOException("Network stream corrupted: invalid magic" +
							"number in current envelope header.");
				}

				int seqNum = this.headerBuffer.getInt();
				JobID jobId = JobID.fromByteBuffer(this.headerBuffer);
				ChannelID sourceId = ChannelID.fromByteBuffer(this.headerBuffer);

				this.currentEnvelope = new Envelope(seqNum, jobId, sourceId);

				int eventsSize = this.headerBuffer.getInt();
				int bufferSize = this.headerBuffer.getInt();

				this.currentEventsBuffer = eventsSize > 0 ? ByteBuffer.allocate(eventsSize) : null;
				this.currentBufferRequestSize = bufferSize > 0 ? bufferSize : 0;

				this.headerBuffer.clear();
			}
		}

		// --------------------------------------------------------------------
		// (2) events (var length)
		// --------------------------------------------------------------------
		if (this.currentEventsBuffer != null) {
			copy(in, this.currentEventsBuffer);

			if (this.currentEventsBuffer.hasRemaining()) {
				return DecoderState.PENDING;
			}
			else {
				this.currentEventsBuffer.flip();
				this.currentEnvelope.setEventsSerialized(this.currentEventsBuffer);
				this.currentEventsBuffer = null;
			}
		}

		// --------------------------------------------------------------------
		// (3) buffer (var length)
		// --------------------------------------------------------------------
		// (a) request a buffer from OUR pool
		if (this.currentBufferRequestSize > 0) {
			JobID jobId = this.currentEnvelope.getJobID();
			ChannelID sourceId = this.currentEnvelope.getSource();
			Buffer buffer = requestBufferForTarget(jobId, sourceId, this.currentBufferRequestSize);

			if (buffer == null) {
				return DecoderState.NO_BUFFER_AVAILABLE;
			}
			else {
				this.currentEnvelope.setBuffer(buffer);
				this.currentDataBuffer = buffer.getMemorySegment().wrap(0, this.currentBufferRequestSize);
				this.currentBufferRequestSize = 0;
			}
		}

		// (b) copy data to OUR buffer
		if (this.currentDataBuffer != null) {
			copy(in, this.currentDataBuffer);

			if (this.currentDataBuffer.hasRemaining()) {
				return DecoderState.PENDING;
			}
			else {
				this.currentDataBuffer = null;
			}
		}

		// if we made it to this point, we completed the envelope;
		// in the other cases we return early with PENDING or NO_BUFFER_AVAILABLE
		return DecoderState.COMPLETE;
	}

	private Buffer requestBufferForTarget(JobID jobId, ChannelID sourceId, int size) throws IOException {
		// Request the buffer from the target buffer provider, which is the
		// InputGate of the receiving InputChannel.
		if (!(jobId.equals(this.lastJobId) && sourceId.equals(this.lastSourceId))) {
			this.lastJobId = jobId;
			this.lastSourceId = sourceId;

			this.currentBufferProvider = this.bufferProviderBroker.getBufferProvider(jobId, sourceId);
		}

		return this.currentBufferProvider.requestBuffer(size);
	}

	/**
	 * Copies min(from.readableBytes(), to.remaining() bytes from Nettys ByteBuf to the Java NIO ByteBuffer.
	 */
	private void copy(ByteBuf src, ByteBuffer dst) {
		// This branch is necessary, because an Exception is thrown if the
		// destination buffer has more remaining (writable) bytes than
		// currently readable from the Netty ByteBuf source.
		if (src.isReadable()) {
			if (src.readableBytes() < dst.remaining()) {
				int oldLimit = dst.limit();

				dst.limit(dst.position() + src.readableBytes());
				src.readBytes(dst);
				dst.limit(oldLimit);
			}
			else {
				src.readBytes(dst);
			}
		}
	}

	/**
	 * Skips over min(in.readableBytes(), toSkip) bytes in the Netty ByteBuf and returns how many bytes remain to be
	 * skipped.
	 *
	 * @return remaining bytes to be skipped
	 */
	private int skipBytes(ByteBuf in, int toSkip) {
		if (toSkip <= in.readableBytes()) {
			in.readBytes(toSkip);
			return 0;
		}

		int remainingToSkip = toSkip - in.readableBytes();
		in.readerIndex(in.readerIndex() + in.readableBytes());

		return remainingToSkip;
	}
}
