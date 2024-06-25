package org.apache.flink.api.common;

import org.apache.flink.api.common.eventtime.Watermark;

public interface WatermarkOutput {
    void emitWatermark(Watermark watermark);
}
