package org.apache.flink.mesos;

import org.apache.mesos.Protos;

import java.net.URL;
import java.util.Arrays;

public class Utils {
	/**
	 * Construct a Mesos environment variable.
     */
	public static Protos.Environment.Variable variable(String name, String value) {
		return Protos.Environment.Variable.newBuilder()
			.setName(name)
			.setValue(value)
			.build();
	}

	/**
	 * Construct a Mesos URI.
     */
	public static Protos.CommandInfo.URI uri(URL url, boolean cacheable) {
		return Protos.CommandInfo.URI.newBuilder()
			.setValue(url.toExternalForm())
			.setExtract(false)
			.setCache(cacheable)
			.build();
	}

	public static Protos.Resource scalar(String name, double value) {
		return Protos.Resource.newBuilder()
			.setName(name)
			.setType(Protos.Value.Type.SCALAR)
			.setScalar(Protos.Value.Scalar.newBuilder().setValue(value))
			.build();
	}

	public static Protos.Value.Range range(long begin, long end) {
		return Protos.Value.Range.newBuilder().setBegin(begin).setEnd(end).build();
	}

	public static Protos.Resource ranges(String name, Protos.Value.Range... ranges) {
		return Protos.Resource.newBuilder()
			.setName(name)
			.setType(Protos.Value.Type.RANGES)
			.setRanges(Protos.Value.Ranges.newBuilder().addAllRange(Arrays.asList(ranges)).build())
			.build();
	}
}
