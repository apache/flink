package eu.stratosphere.pact.testing;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultDataInputView;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultDataOutputView;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemorySegment;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemorySegmentView;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager.MemorySegmentDescriptor;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultRandomAccessView;
import eu.stratosphere.nephele.template.AbstractInvokable;

public class MockMemoryManager implements MemoryManager {

	@Override
	public List<MemorySegment> allocate(AbstractInvokable owner, final long totalMemory, int minNumSegments,
			int minSegmentSize) throws MemoryAllocationException {
		final byte[] memory = new byte[(int) totalMemory];

		MemorySegmentDescriptor memorySegmentDescriptor = new MemorySegmentDescriptor(owner, memory, memory.length,
			0, memory.length);

		MemorySegment defaultMemorySegment = new DefaultMemorySegment(memorySegmentDescriptor,
			new DefaultRandomAccessView(memorySegmentDescriptor),
			new DefaultDataInputView(memorySegmentDescriptor),
			new DefaultDataOutputView(memorySegmentDescriptor));
		return new ArrayList<MemorySegment>(Arrays.asList(defaultMemorySegment));
	}

	@Override
	public void release(MemorySegment segment) {
	}

	@Override
	public <T extends MemorySegment> void release(Collection<T> segments) {
	}

	@Override
	public <T extends MemorySegment> void releaseAll(AbstractInvokable task) {
	}

	@Override
	public void shutdown() {
	}

	@Override
	public boolean verifyEmpty() {
		return true;
	}

}
