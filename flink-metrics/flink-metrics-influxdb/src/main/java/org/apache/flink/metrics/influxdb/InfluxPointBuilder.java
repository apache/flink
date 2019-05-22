package org.apache.flink.metrics.influxdb;

import org.influxdb.dto.Point;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

/**
 * Wrapper around influxDB Point.builder which excludes non influx compatible values (i.e. +-infininty) from
 * being sent. (Influx would throw an error and ignore them anyways).
 */
public class InfluxPointBuilder {

	private Point.Builder builder;

	/**
	 * Create a new wrapper around the InfluxDB Point.Builder.
	 */
	public InfluxPointBuilder(MeasurementInfo info, Instant timestamp) {
		builder = Point.measurement(info.getName())
			.tag(info.getTags())
			.time(timestamp.toEpochMilli(), TimeUnit.MILLISECONDS);
	}

	/**
	 * wrap {@link org.influxdb.dto.Point.Builder#addField(String,boolean)}.
	 */
	public InfluxPointBuilder addField(final String field, final boolean value) {
		builder.addField(field, value);
		return this;
	}

	/**
	 * wrap {@link org.influxdb.dto.Point.Builder#addField(String,long)}.
	 */
	public InfluxPointBuilder addField(final String field, final long value) {
		builder.addField(field, value);
		return this;
	}

	/**
	 * wrap {@link org.influxdb.dto.Point.Builder#addField(String,double)}.
	 */
	public InfluxPointBuilder addField(final String field, final double value) {
		if (!Double.isInfinite(value)) {
			builder.addField(field, value);
		}
		return this;
	}

	/**
	 * wrap {@link org.influxdb.dto.Point.Builder#addField(String,Number)}.
	 */
	public InfluxPointBuilder addField(final String field, final Number value) {
		if (value == null) {
			return this;
		}

		if ((value instanceof Double) && Double.isInfinite(value.doubleValue())) {
			return this;
		}

		if ((value instanceof Float) && Float.isInfinite(value.floatValue())) {
			return this;
		}

		builder.addField(field, value);
		return this;
	}

	/**
	 * wrap {@link org.influxdb.dto.Point.Builder#addField(String,String)}.
	 */
	public InfluxPointBuilder addField(final String field, final String value) {
		builder.addField(field, value);
		return this;
	}

	/**
	 * wrap {@link org.influxdb.dto.Point.Builder#build()} which can return null as well.
	 */
	public Point build() {
		if (builder.hasFields()) {
			return builder.build();
		}

		// If there are no fields, a measurement can't be inserted to influx and we thus won't send the metric.
		return null;
	}
}
