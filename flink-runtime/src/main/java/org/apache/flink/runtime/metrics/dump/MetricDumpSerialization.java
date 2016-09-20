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

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.metrics.Meter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
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

	//-------------------------------------------------------------------------
	// Serialization
	//-------------------------------------------------------------------------
	public static class MetricDumpSerializer {
		private ByteArrayOutputStream baos = new ByteArrayOutputStream(4096);
		private DataOutputStream dos = new DataOutputStream(baos);

		/**
		 * Serializes the given metrics and returns the resulting byte array.
		 *
		 * @param counters   counters to serialize
		 * @param gauges     gauges to serialize
		 * @param histograms histograms to serialize
		 * @return byte array containing the serialized metrics
		 * @throws IOException
		 */
		public byte[] serialize(
			Map<Counter, Tuple2<QueryScopeInfo, String>> counters,
			Map<Gauge<?>, Tuple2<QueryScopeInfo, String>> gauges,
			Map<Histogram, Tuple2<QueryScopeInfo, String>> histograms,
			Map<Meter, Tuple2<QueryScopeInfo, String>> meters) throws IOException {
				
			baos.reset();
			dos.writeInt(counters.size());
			dos.writeInt(gauges.size());
			dos.writeInt(histograms.size());
			dos.writeInt(meters.size());

			for (Map.Entry<Counter, Tuple2<QueryScopeInfo, String>> entry : counters.entrySet()) {
				serializeMetricInfo(dos, entry.getValue().f0);
				serializeString(dos, entry.getValue().f1);
				serializeCounter(dos, entry.getKey());
			}

			for (Map.Entry<Gauge<?>, Tuple2<QueryScopeInfo, String>> entry : gauges.entrySet()) {
				serializeMetricInfo(dos, entry.getValue().f0);
				serializeString(dos, entry.getValue().f1);
				serializeGauge(dos, entry.getKey());
			}

			for (Map.Entry<Histogram, Tuple2<QueryScopeInfo, String>> entry : histograms.entrySet()) {
				serializeMetricInfo(dos, entry.getValue().f0);
				serializeString(dos, entry.getValue().f1);
				serializeHistogram(dos, entry.getKey());
			}

			for (Map.Entry<Meter, Tuple2<QueryScopeInfo, String>> entry : meters.entrySet()) {
				serializeMetricInfo(dos, entry.getValue().f0);
				serializeString(dos, entry.getValue().f1);
				serializeMeter(dos, entry.getKey());
			}
			return baos.toByteArray();
		}

		public void close() {
			try {
				dos.close();
			} catch (Exception e) {
				LOG.debug("Failed to close OutputStream.", e);
			}
			try {
				baos.close();
			} catch (Exception e) {
				LOG.debug("Failed to close OutputStream.", e);
			}
		}
	}

	private static void serializeMetricInfo(DataOutputStream dos, QueryScopeInfo info) throws IOException {
		serializeString(dos, info.scope);
		dos.writeByte(info.getCategory());
		switch (info.getCategory()) {
			case INFO_CATEGORY_JM:
				break;
			case INFO_CATEGORY_TM:
				String tmID = ((QueryScopeInfo.TaskManagerQueryScopeInfo) info).taskManagerID;
				serializeString(dos, tmID);
				break;
			case INFO_CATEGORY_JOB:
				QueryScopeInfo.JobQueryScopeInfo jobInfo = (QueryScopeInfo.JobQueryScopeInfo) info;
				serializeString(dos, jobInfo.jobID);
				break;
			case INFO_CATEGORY_TASK:
				QueryScopeInfo.TaskQueryScopeInfo taskInfo = (QueryScopeInfo.TaskQueryScopeInfo) info;
				serializeString(dos, taskInfo.jobID);
				serializeString(dos, taskInfo.vertexID);
				dos.writeInt(taskInfo.subtaskIndex);
				break;
			case INFO_CATEGORY_OPERATOR:
				QueryScopeInfo.OperatorQueryScopeInfo operatorInfo = (QueryScopeInfo.OperatorQueryScopeInfo) info;
				serializeString(dos, operatorInfo.jobID);
				serializeString(dos, operatorInfo.vertexID);
				dos.writeInt(operatorInfo.subtaskIndex);
				serializeString(dos, operatorInfo.operatorName);
				break;
		}
	}

	private static void serializeString(DataOutputStream dos, String string) throws IOException {
		byte[] bytes = string.getBytes();
		dos.writeInt(bytes.length);
		dos.write(bytes);
	}

	private static void serializeCounter(DataOutputStream dos, Counter counter) throws IOException {
		dos.writeLong(counter.getCount());
	}

	private static void serializeGauge(DataOutputStream dos, Gauge<?> gauge) throws IOException {
		serializeString(dos, gauge.getValue().toString());
	}

	private static void serializeHistogram(DataOutputStream dos, Histogram histogram) throws IOException {
		HistogramStatistics stat = histogram.getStatistics();

		dos.writeLong(stat.getMin());
		dos.writeLong(stat.getMax());
		dos.writeDouble(stat.getMean());
		dos.writeDouble(stat.getQuantile(0.5));
		dos.writeDouble(stat.getStdDev());
		dos.writeDouble(stat.getQuantile(0.75));
		dos.writeDouble(stat.getQuantile(0.90));
		dos.writeDouble(stat.getQuantile(0.95));
		dos.writeDouble(stat.getQuantile(0.98));
		dos.writeDouble(stat.getQuantile(0.99));
		dos.writeDouble(stat.getQuantile(0.999));
	}

	private static void serializeMeter(DataOutputStream dos, Meter meter) throws IOException {
		dos.writeDouble(meter.getRate());
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
		 * @throws IOException
		 */
		public List<MetricDump> deserialize(byte[] data) throws IOException {
			ByteArrayInputStream bais = new ByteArrayInputStream(data);
			DataInputStream dis = new DataInputStream(bais);

			int numCounters = dis.readInt();
			int numGauges = dis.readInt();
			int numHistograms = dis.readInt();
			int numMeters = dis.readInt();

			List<MetricDump> metrics = new ArrayList<>(numCounters + numGauges + numHistograms);

			for (int x = 0; x < numCounters; x++) {
				metrics.add(deserializeCounter(dis));
			}

			for (int x = 0; x < numGauges; x++) {
				metrics.add(deserializeGauge(dis));
			}

			for (int x = 0; x < numHistograms; x++) {
				metrics.add(deserializeHistogram(dis));
			}

			for (int x = 0; x < numMeters; x++) {
				metrics.add(deserializeMeter(dis));
			}

			return metrics;
		}
	}

	private static String deserializeString(DataInputStream dis) throws IOException {
		int stringLength = dis.readInt();
		byte[] bytes = new byte[stringLength];
		dis.readFully(bytes);
		return new String(bytes);
	}

	private static MetricDump.CounterDump deserializeCounter(DataInputStream dis) throws IOException {
		QueryScopeInfo scope = deserializeMetricInfo(dis);
		String name = deserializeString(dis);
		return new MetricDump.CounterDump(scope, name, dis.readLong());
	}

	private static MetricDump.GaugeDump deserializeGauge(DataInputStream dis) throws IOException {
		QueryScopeInfo scope = deserializeMetricInfo(dis);
		String name = deserializeString(dis);
		String value = deserializeString(dis);
		return new MetricDump.GaugeDump(scope, name, value);
	}

	private static MetricDump.HistogramDump deserializeHistogram(DataInputStream dis) throws IOException {
		QueryScopeInfo info = deserializeMetricInfo(dis);
		String name = deserializeString(dis);
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

	private static MetricDump.MeterDump deserializeMeter(DataInputStream dis) throws IOException {
		QueryScopeInfo info = deserializeMetricInfo(dis);
		String name = deserializeString(dis);
		double rate = dis.readDouble();
		return new MetricDump.MeterDump(info, name, rate);
	}

	private static QueryScopeInfo deserializeMetricInfo(DataInputStream dis) throws IOException {
		String jobID;
		String vertexID;
		int subtaskIndex;

		String scope = deserializeString(dis);
		byte cat = dis.readByte();
		switch (cat) {
			case INFO_CATEGORY_JM:
				return new QueryScopeInfo.JobManagerQueryScopeInfo(scope);
			case INFO_CATEGORY_TM:
				String tmID = deserializeString(dis);
				return new QueryScopeInfo.TaskManagerQueryScopeInfo(tmID, scope);
			case INFO_CATEGORY_JOB:
				jobID = deserializeString(dis);
				return new QueryScopeInfo.JobQueryScopeInfo(jobID, scope);
			case INFO_CATEGORY_TASK:
				jobID = deserializeString(dis);
				vertexID = deserializeString(dis);
				subtaskIndex = dis.readInt();
				return new QueryScopeInfo.TaskQueryScopeInfo(jobID, vertexID, subtaskIndex, scope);
			case INFO_CATEGORY_OPERATOR:
				jobID = deserializeString(dis);
				vertexID = deserializeString(dis);
				subtaskIndex = dis.readInt();
				String operatorName = deserializeString(dis);
				return new QueryScopeInfo.OperatorQueryScopeInfo(jobID, vertexID, subtaskIndex, operatorName, scope);
			default:
				throw new IOException("sup");
		}
	}
}
