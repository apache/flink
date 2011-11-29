package eu.stratosphere.nephele.streaming.buffers;

import eu.stratosphere.nephele.managementgraph.ManagementEdge;

public class BufferSizeHistoryEntry {
	private int entryIndex;

	private ManagementEdge edge;

	private long timestamp;

	private int bufferSize;

	public BufferSizeHistoryEntry(int entryIndex, ManagementEdge edge, long timestamp, int bufferSize) {
		this.entryIndex = entryIndex;
		this.edge = edge;
		this.timestamp = timestamp;
		this.bufferSize = bufferSize;
	}

	public int getEntryIndex() {
		return entryIndex;
	}

	public ManagementEdge getEdge() {
		return edge;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public int getBufferSize() {
		return bufferSize;
	}

}
