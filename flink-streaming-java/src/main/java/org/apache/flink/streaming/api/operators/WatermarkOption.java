package org.apache.flink.streaming.api.operators;

/**
 *
 * Defines the watermark options for all "TwoInputStreamOperator".
 *
 * The default value used by the StreamOperator is {@link #ALL}, which means that
 * the operator receive two stream watermark. {@link #STREAM1} and {@link #STREAM2} mean that
 * the operator receive one stream watermatrk.
 *
 */
public enum WatermarkOption {
	STREAM1,
	STREAM2,
	ALL
}
