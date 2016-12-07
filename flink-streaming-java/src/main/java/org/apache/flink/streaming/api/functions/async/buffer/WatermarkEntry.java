package org.apache.flink.streaming.api.functions.async.buffer;

import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * {@link AsyncCollectorBuffer} entry for {@link Watermark}
 *
 */
public class WatermarkEntry<OUT> extends AbstractBufferEntry<OUT> {
	public WatermarkEntry(Watermark watermark) {
		super(watermark);
	}

	@Override
	public boolean isDone() {
		return true;
	}
}
