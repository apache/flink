package eu.stratosphere.pact.iterative.nephele.util;

import java.io.EOFException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.pact.runtime.io.AbstractPagedInputViewV2;
import eu.stratosphere.pact.runtime.io.AbstractPagedOutputViewV2;
import eu.stratosphere.pact.runtime.io.MemorySegmentSource;

/**
 *
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class SerializedPassthroughUpdateBuffer
{	
	private static final int HEADER_LENGTH = 4;
	
	private final ArrayBlockingQueue<MemorySegment> emptyBuffers;
	
	private final ArrayBlockingQueue<MemorySegment> fullBuffers;
	
	private WriteEnd writeEnd;
	
	private ReadEnd readEnd;
	
	private volatile boolean closed;
	
	private final java.util.concurrent.atomic.AtomicInteger count;
	private volatile boolean blocks = false;
	
	
	public SerializedPassthroughUpdateBuffer(List<MemorySegment> memSegments, int segmentSize)
	{
		count = new AtomicInteger();
		final MemorySegmentSource fullBufferSource;
		final MemorySegmentSource emptyBufferSource;
		
		this.emptyBuffers = new ArrayBlockingQueue<MemorySegment>(memSegments.size()+1);
		this.fullBuffers = new ArrayBlockingQueue<MemorySegment>(memSegments.size()+1);
		
		fullBufferSource = getTimeoutSource((ArrayBlockingQueue<MemorySegment>) this.fullBuffers);
		emptyBufferSource = getBlockingSource((ArrayBlockingQueue<MemorySegment>) this.emptyBuffers);
		
		this.emptyBuffers.addAll(memSegments);
		
		// views may only be instantiated after the memory has been loaded
		this.readEnd = new ReadEnd(this.emptyBuffers, fullBufferSource, segmentSize);
		this.writeEnd = new WriteEnd(this.fullBuffers, emptyBufferSource, segmentSize);
	}
	
	public void incCount() {
		count.incrementAndGet();
	}
	
	public void decCount() {
		count.decrementAndGet();
	}
	
	public int getCount() {
		return count.get();
	}
	
	public WriteEnd getWriteEnd() {
		return writeEnd;
	}
	
	public ReadEnd getReadEnd() {
		return readEnd;
	}
	
	public void flush() throws IOException {
		this.writeEnd.flush();
	}
	
	public void close() {
		this.closed = true;
	}
	
	private MemorySegmentSource getBlockingSource(final ArrayBlockingQueue<MemorySegment> source)
	{
		return new MemorySegmentSource() {
			@Override
			public MemorySegment nextSegment() {
				if (SerializedPassthroughUpdateBuffer.this.closed && source.isEmpty()) {
					return null;
				} else {
					try {
						return source.take();
					} catch (InterruptedException e) {
						throw new RuntimeException("Interrupted!!", e);
					}
				}
			}
		};
	}
	
	private MemorySegmentSource getTimeoutSource(final ArrayBlockingQueue<MemorySegment> source)
	{
		final SerializedPassthroughUpdateBuffer buffer = this;
		
		return new MemorySegmentSource() {
			int count = 0;
			
			SerializedPassthroughUpdateBuffer buf = buffer;
			
			@Override
			public MemorySegment nextSegment() {
				if (SerializedPassthroughUpdateBuffer.this.closed && source.isEmpty()) {
					return null;
				} else {
					try {
						MemorySegment seg;
						if(count < 2) {
							seg = source.take();
						} else {
							seg = source.poll();
						}
						
						if(seg == null) {
							synchronized (buf) {
								//Are there some records already available?
								if(buf.getCount() > 0) {
									buf.writeEnd.flush();
									seg = source.take();
								} else {
									//Wait up to 2 seconds to see whether some arrived
									if(!blocks) {
										blocks = true;
									}
									buf.wait(5000);
									if(buf.getCount() > 0) {
										buf.writeEnd.flush();
										seg = source.take();
									} else {
										return null;
									}
								}
							}
						}
						
						count++;
						return seg;
					} catch (InterruptedException e) {
						throw new RuntimeException("FifoBuffer was interrupted waiting next memory segment.");
					} catch (IOException e) {
						throw new RuntimeException("Boooo");
					}
				}
			}
		};
	}

	public boolean isBlocking() {
		return blocks;
	}
	
	protected static final class ReadEnd extends AbstractPagedInputViewV2
	{
		private final BlockingQueue<MemorySegment> emptyBufferTarget;
		
		private final MemorySegmentSource fullBufferSource;
		
		ReadEnd(BlockingQueue<MemorySegment> emptyBufferTarget, MemorySegmentSource fullBufferSource, int segmentSize)
		{
			super(HEADER_LENGTH);
			
			this.emptyBufferTarget = emptyBufferTarget;
			this.fullBufferSource = fullBufferSource;
			
			seekInput(emptyBufferTarget.poll(), segmentSize, segmentSize);
		}
		
		/* (non-Javadoc)
		 * @see eu.stratosphere.pact.runtime.io.AbstractPagedInputViewV2#nextSegment(eu.stratosphere.nephele.services.memorymanager.MemorySegment)
		 */
		@Override
		protected MemorySegment nextSegment(MemorySegment current) throws IOException {
			try {
				this.emptyBufferTarget.put(current);
			} catch(Exception ex) {
				throw new RuntimeException(ex);
			}
			
			final MemorySegment seg = this.fullBufferSource.nextSegment();
			if (seg != null) {
				return seg;
			} else {
				throw new EOFException();
			}
		}

		/* (non-Javadoc)
		 * @see eu.stratosphere.pact.runtime.io.AbstractPagedInputViewV2#getLimitForSegment(eu.stratosphere.nephele.services.memorymanager.MemorySegment)
		 */
		@Override
		protected int getLimitForSegment(MemorySegment segment) throws IOException {
			return segment.getInt(0);
		}
	}
	
	protected static final class WriteEnd extends AbstractPagedOutputViewV2
	{	
		private final BlockingQueue<MemorySegment> fullBufferTarget;
		
		private final MemorySegmentSource emptyBufferSource;
		
		WriteEnd(BlockingQueue<MemorySegment> fullBufferTarget, MemorySegmentSource emptyBufferSource, int segmentSize)
		{
			super(emptyBufferSource.nextSegment(), segmentSize, HEADER_LENGTH);
			
			this.fullBufferTarget = fullBufferTarget;
			this.emptyBufferSource = emptyBufferSource;
		}

		/* (non-Javadoc)
		 * @see eu.stratosphere.pact.runtime.io.AbstractPagedOutputViewV2#nextSegment(eu.stratosphere.nephele.services.memorymanager.MemorySegment, int)
		 */
		@Override
		protected MemorySegment nextSegment(MemorySegment current, int positionInCurrent) throws IOException
		{
			current.putInt(0, positionInCurrent);
			
			try {
				this.fullBufferTarget.put(current);
			} catch (Exception ex) {
				throw new RuntimeException(ex);
			}
			
			return this.emptyBufferSource.nextSegment();
		}
		
		void flush() throws IOException
		{
			advance();
		}
	}
}