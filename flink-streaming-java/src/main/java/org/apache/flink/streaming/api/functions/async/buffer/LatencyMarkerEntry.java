package org.apache.flink.streaming.api.functions.async.buffer;

import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;

/**
 * {@link AsyncCollectorBuffer} entry for {@link LatencyMarker}
 *
 */
public class LatencyMarkerEntry<OUT> extends AbstractBufferEntry<OUT> {
	public LatencyMarkerEntry(LatencyMarker marker) {
		super(marker);
	}

	@Override
	public boolean isDone() {
		return true;
	}
}
