package org.apache.flink.api.common;

import org.apache.flink.api.common.eventtime.GenericWatermark;

public interface WatermarkOutput {
    void emitWatermark(GenericWatermark watermark);
}
