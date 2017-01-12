/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.runtime.metrics.dump;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.metrics.Meter;
import org.apache.flink.runtime.util.DataInputDeserializer;
import org.apache.flink.runtime.util.DataOutputSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.flink.runtime.metrics.dump.QueryScopeInfo.INFO_CATEGORY_JM;
import static org.apache.flink.runtime.metrics.dump.QueryScopeInfo.INFO_CATEGORY_JOB;
import static org.apache.flink.runtime.metrics.dump.QueryScopeInfo.INFO_CATEGORY_OPERATOR;
import static org.apache.flink.runtime.metrics.dump.QueryScopeInfo.INFO_CATEGORY_TASK;
import static org.apache.flink.runtime.metrics.dump.QueryScopeInfo.INFO_CATEGORY_TM;

/**
 * Utility class for the serialization of metrics.
 */
public class MetricDumpSerialization {
	private static final Logger LOG = LoggerFactory.getLogger(MetricDumpSerialization.class);

	private MetricDumpSerialization() {
	}

	public static class MetricSerializationResult {
		public final byte[] data;
		public final int numCounters;
		public final int numGauges;
		public final int numMeters;
		public final int numHistograms;
		
		public MetricSerializationResult(byte[] data, int numCounters, int numGauges, int numMeters, int numHistograms) {
			this.data = data;
			this.numCounters = numCounters;
			this.numGauges = numGauges;
			this.numMeters = numMeters;
			this.numHistograms = numHistograms;
		}
	}

	//-------------------------------------------------------------------------
	// Serialization
	//-------------------------------------------------------------------------
	public static class MetricDumpSerializer {
		private DataOutputSerializer buffer = new DataOutputSerializer(1024 * 32);

		/**
		 * Serializes the given metrics and returns the resulting byte array.
		 *
		 * @param counters   counters to serialize
		 * @param gauges     gauges to serialize
		 * @param histograms histograms to serialize
		 * @return byte array containing the serialized metrics
		 */
		public MetricSerializationResult serialize(
			Map<Counter, Tuple2<QueryScopeInfo, String>> counters,
			Map<Gauge<?>, Tuple2<QueryScopeInfo, String>> gauges,
			Map<Histogram, Tuple2<QueryScopeInfo, String>> histograms,
			Map<Meter, Tuple2<QueryScopeInfo, String>> meters) {

			buffer.clear();

			int numCounters = 0;
			for (Map.Entry<Counter, Tuple2<QueryScopeInfo, String>> entry : counters.entrySet()) {
				try {
					serializeCounter(buffer, entry.getValue().f0, entry.getValue().f1, entry.getKey());
					numCounters++;
				} catch (Exception e) {
					LOG.warn("Failed to serialize counter.", e);
				}
			}

			int numGauges = 0;
			for (Map.Entry<Gauge<?>, Tuple2<QueryScopeInfo, String>> entry : gauges.entrySet()) {
				try {
					serializeGauge(buffer, entry.getValue().f0, entry.getValue().f1, entry.getKey());
					numGauges++;
				} catch (Exception e) {
					LOG.warn("Failed to serialize gauge.", e);
				}
			}

			int numHistograms = 0;
			for (Map.Entry<Histogram, Tuple2<QueryScopeInfo, String>> entry : histograms.entrySet()) {
				try {
					serializeHistogram(buffer, entry.getValue().f0, entry.getValue().f1, entry.getKey());
					numHistograms++;
				} catch (Exception e) {
					LOG.warn("Failed to serialize histogram.", e);
				}
			}

			int numMeters = 0;
			for (Map.Entry<Meter, Tuple2<QueryScopeInfo, String>> entry : meters.entrySet()) {
				try {
					serializeMeter(buffer, entry.getValue().f0, entry.getValue().f1, entry.getKey());
					numMeters++;
				} catch (Exception e) {
					LOG.warn("Failed to serialize meter.", e);
				}
			}
			return new MetricSerializationResult(buffer.getCopyOfBuffer(), numCounters, numGauges, numMeters, numHistograms);
		}

		public void close() {
			buffer = null;
		}
	}

	private static void serializeMetricInfo(DataOutput out, QueryScopeInfo info) throws IOException {
		out.writeUTF(info.scope);
		out.writeByte(info.getCategory());
		switch (info.getCategory()) {
			case INFO_CATEGORY_JM:
				break;
			case INFO_CATEGORY_TM:
				String tmID = ((QueryScopeInfo.TaskManagerQueryScopeInfo) info).taskManagerID;
				out.writeUTF(tmID);
				break;
			case INFO_CATEGORY_JOB:
				QueryScopeInfo.JobQueryScopeInfo jobInfo = (QueryScopeInfo.JobQueryScopeInfo) info;
				out.writeUTF(jobInfo.jobID);
				break;
			case INFO_CATEGORY_TASK:
				QueryScopeInfo.TaskQueryScopeInfo taskInfo = (QueryScopeInfo.TaskQueryScopeInfo) info;
				out.writeUTF(taskInfo.jobID);
				out.writeUTF(taskInfo.vertexID);
				out.writeInt(taskInfo.subtaskIndex);
				break;
			case INFO_CATEGORY_OPERATOR:
				QueryScopeInfo.OperatorQueryScopeInfo operatorInfo = (QueryScopeInfo.OperatorQueryScopeInfo) info;
				out.writeUTF(operatorInfo.jobID);
				out.writeUTF(operatorInfo.vertexID);
				out.writeInt(operatorInfo.subtaskIndex);
				out.writeUTF(operatorInfo.operatorName);
				break;
			default:
				throw new IOException("Unknown scope category: " + info.getCategory());
		}
	}

	private static void serializeCounter(DataOutput out, QueryScopeInfo info, String name, Counter counter) throws IOException {
		long count = counter.getCount();
		serializeMetricInfo(out, info);
		out.writeUTF(name);
		out.writeLong(count);
	}

	private static void serializeGauge(DataOutput out, QueryScopeInfo info, String name, Gauge<?> gauge) throws IOException {
		Object value = gauge.getValue();
		if (value == null) {
			throw new NullPointerException("Value returned by gauge " + name + " was null.");
		}
		String stringValue = gauge.getValue().toString();
		if (stringValue == null) {
			throw new NullPointerException("toString() of the value returned by gauge " + name + " returned null.");
		}
		serializeMetricInfo(out, info);
		out.writeUTF(name);
		out.writeUTF(stringValue);
	}

	private static void serializeHistogram(DataOutput out, QueryScopeInfo info, String name, Histogram histogram) throws IOException {
		HistogramStatistics stat = histogram.getStatistics();
		long min = stat.getMin();
		long max = stat.getMax();
		double mean = stat.getMean();
		double mediam = stat.getQuantile(0.5);
		double stddev = stat.getStdDev();
		double p75 = stat.getQuantile(0.75);
		double p90 = stat.getQuantile(0.90);
		double p95 = stat.getQuantile(0.95);
		double p98 = stat.getQuantile(0.98);
		double p99 = stat.getQuantile(0.99);
		double p999 = stat.getQuantile(0.999);

		serializeMetricInfo(out, info);
		out.writeUTF(name);
		out.writeLong(min);
		out.writeLong(max);
		out.writeDouble(mean);
		out.writeDouble(mediam);
		out.writeDouble(stddev);
		out.writeDouble(p75);
		out.writeDouble(p90);
		out.writeDouble(p95);
		out.writeDouble(p98);
		out.writeDouble(p99);
		out.writeDouble(p999);
	}

	private static void serializeMeter(DataOutput out, QueryScopeInfo info, String name, Meter meter) throws IOException {
		serializeMetricInfo(out, info);
		out.writeUTF(name);
		out.writeDouble(meter.getRate());
	}

	//-------------------------------------------------------------------------
	// Deserialization
	//-------------------------------------------------------------------------
	public static class MetricDumpDeserializer {
		/**
		 * De-serializes metrics from the given byte array and returns them as a list of {@link MetricDump}.
		 *
		 * @param data serialized metrics
		 * @return A list containing the deserialized metrics.
		 */

		public List<MetricDump> deserialize(MetricDumpSerialization.MetricSerializationResult data) {
			DataInputView in = new DataInputDeserializer(data.data, 0, data.data.length);

			List<MetricDump> metrics = new ArrayList<>(data.numCounters + data.numGauges + data.numHistograms + data.numMeters);

			for (int x = 0; x < data.numCounters; x++) {
				try {
					metrics.add(deserializeCounter(in));
				} catch (Exception e) {
					LOG.warn("Failed to deserialize counter.", e);
				}
			}

			for (int x = 0; x < data.numGauges; x++) {
				try {
					metrics.add(deserializeGauge(in));
				} catch (Exception e) {
					LOG.warn("Failed to deserialize gauge.", e);
				}
			}

			for (int x = 0; x < data.numHistograms; x++) {
				try {
					metrics.add(deserializeHistogram(in));
				} catch (Exception e) {
					LOG.warn("Failed to deserialize histogram.", e);
				}
			}

			for (int x = 0; x < data.numMeters; x++) {
				try {
					metrics.add(deserializeMeter(in));
				} catch (Exception e) {
					LOG.warn("Failed to deserialize meter.", e);
				}
			}
			return metrics;
		}
	}


	private static MetricDump.CounterDump deserializeCounter(DataInputView dis) throws IOException {
		QueryScopeInfo scope = deserializeMetricInfo(dis);
		String name = dis.readUTF();
		long count = dis.readLong();
		return new MetricDump.CounterDump(scope, name, count);
	}

	private static MetricDump.GaugeDump deserializeGauge(DataInputView dis) throws IOException {
		QueryScopeInfo scope = deserializeMetricInfo(dis);
		String name = dis.readUTF();
		String value = dis.readUTF();
		return new MetricDump.GaugeDump(scope, name, value);
	}

	private static MetricDump.HistogramDump deserializeHistogram(DataInputView dis) throws IOException {
		QueryScopeInfo info = deserializeMetricInfo(dis);
		String name = dis.readUTF();
		long min = dis.readLong();
		long max = dis.readLong();
		double mean = dis.readDouble();
		double median = dis.readDouble();
		double stddev = dis.readDouble();
		double p75 = dis.readDouble();
		double p90 = dis.readDouble();
		double p95 = dis.readDouble();
		double p98 = dis.readDouble();
		double p99 = dis.readDouble();
		double p999 = dis.readDouble();
		return new MetricDump.HistogramDump(info, name, min, max, mean, median, stddev, p75, p90, p95, p98, p99, p999);
	}

	private static MetricDump.MeterDump deserializeMeter(DataInputView dis) throws IOException {
		QueryScopeInfo info = deserializeMetricInfo(dis);
		String name = dis.readUTF();
		double rate = dis.readDouble();
		return new MetricDump.MeterDump(info, name, rate);
	}

	private static QueryScopeInfo deserializeMetricInfo(DataInput dis) throws IOException {
		String jobID;
		String vertexID;
		int subtaskIndex;

		String scope = dis.readUTF();
		byte cat = dis.readByte();
		switch (cat) {
			case INFO_CATEGORY_JM:
				return new QueryScopeInfo.JobManagerQueryScopeInfo(scope);
			case INFO_CATEGORY_TM:
				String tmID = dis.readUTF();
				return new QueryScopeInfo.TaskManagerQueryScopeInfo(tmID, scope);
			case INFO_CATEGORY_JOB:
				jobID = dis.readUTF();
				return new QueryScopeInfo.JobQueryScopeInfo(jobID, scope);
			case INFO_CATEGORY_TASK:
				jobID = dis.readUTF();
				vertexID = dis.readUTF();
				subtaskIndex = dis.readInt();
				return new QueryScopeInfo.TaskQueryScopeInfo(jobID, vertexID, subtaskIndex, scope);
			case INFO_CATEGORY_OPERATOR:
				jobID = dis.readUTF();
				vertexID = dis.readUTF();
				subtaskIndex = dis.readInt();
				String operatorName = dis.readUTF();
				return new QueryScopeInfo.OperatorQueryScopeInfo(jobID, vertexID, subtaskIndex, operatorName, scope);
			default:
				throw new IOException("Unknown scope category: " + cat);
		}
	}
}
