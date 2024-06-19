package org.apache.flink.api.common.eventtime;

import org.apache.flink.api.common.WatermarkCombiner;
import org.apache.flink.api.common.WatermarkOutput;

public class UnsupportedWatermarkCombiner implements WatermarkCombiner {
    @Override
    public void combineWatermark(
            GenericWatermark watermark,
            Context context,
            WatermarkOutput output) throws Exception {
        throw new UnsupportedOperationException(
                "Watermark combination is not supported for the given watermark.");
    }
}
