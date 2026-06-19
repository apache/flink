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
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.flink.runtime.metrics.dump.QueryScopeInfo.INFO_CATEGORY_JM;
import static org.apache.flink.runtime.metrics.dump.QueryScopeInfo.INFO_CATEGORY_JM_OPERATOR;
import static org.apache.flink.runtime.metrics.dump.QueryScopeInfo.INFO_CATEGORY_JOB;
import static org.apache.flink.runtime.metrics.dump.QueryScopeInfo.INFO_CATEGORY_OPERATOR;
import static org.apache.flink.runtime.metrics.dump.QueryScopeInfo.INFO_CATEGORY_TASK;
import static org.apache.flink.runtime.metrics.dump.QueryScopeInfo.INFO_CATEGORY_TM;

/** Utility class for the serialization of metrics. */
public class MetricDumpSerialization {

    private static final Logger LOG = LoggerFactory.getLogger(MetricDumpSerialization.class);

    private MetricDumpSerialization() {}

    /**
     * This class encapsulates all serialized metrics and a count for each metric type.
     *
     * <p>The counts are stored separately from the metrics since the final count for any given type
     * can only be determined after all metrics of that type were serialized. Storing them together
     * in a single byte[] would require an additional copy of all serialized metrics, as you would
     * first have to serialize the metrics into a temporary buffer to calculate the counts, write
     * the counts to the final output and copy all metrics from the temporary buffer.
     *
     * <p>Note that while one could implement the serialization in such a way so that at least 1
     * byte (a validity flag) is written for each metric, this would require more bandwidth due to
     * the sheer number of metrics.
     */
    public static class MetricSerializationResult implements Serializable {

        private static final long serialVersionUID = 6928770855951536906L;

        public final byte[] serializedCounters;
        public final byte[] serializedGauges;
        public final byte[] serializedMeters;
        public final byte[] serializedHistograms;

        public final int numCounters;
        public final int numGauges;
        public final int numMeters;
        public final int numHistograms;

        public MetricSerializationResult(
                byte[] serializedCounters,
                byte[] serializedGauges,
                byte[] serializedMeters,
                byte[] serializedHistograms,
                int numCounters,
                int numGauges,
                int numMeters,
                int numHistograms) {

            Preconditions.checkNotNull(serializedCounters);
            Preconditions.checkNotNull(serializedGauges);
            Preconditions.checkNotNull(serializedMeters);
            Preconditions.checkNotNull(serializedHistograms);
            Preconditions.checkArgument(numCounters >= 0);
            Preconditions.checkArgument(numGauges >= 0);
            Preconditions.checkArgument(numMeters >= 0);
            Preconditions.checkArgument(numHistograms >= 0);
            this.serializedCounters = serializedCounters;
            this.serializedGauges = serializedGauges;
            this.serializedMeters = serializedMeters;
            this.serializedHistograms = serializedHistograms;
            this.numCounters = numCounters;
            this.numGauges = numGauges;
            this.numMeters = numMeters;
            this.numHistograms = numHistograms;
        }
    }

    // -------------------------------------------------------------------------
    // Serialization
    // -------------------------------------------------------------------------

    /** Serializes a set of metrics into a {@link MetricSerializationResult}. */
    public static class MetricDumpSerializer {

        private DataOutputSerializer countersBuffer = new DataOutputSerializer(1024 * 8);
        private DataOutputSerializer gaugesBuffer = new DataOutputSerializer(1024 * 8);
        private DataOutputSerializer metersBuffer = new DataOutputSerializer(1024 * 8);
        private DataOutputSerializer histogramsBuffer = new DataOutputSerializer(1024 * 8);

        /**
         * Serializes the given metrics and returns the resulting byte array.
         *
         * <p>Should a {@link Metric} accessed in this method throw an exception it will be omitted
         * from the returned {@link MetricSerializationResult}.
         *
         * <p>If the serialization of any primitive or String fails then the returned {@link
         * MetricSerializationResult} is partially corrupted. Such a result can be deserialized
         * safely by {@link MetricDumpDeserializer#deserialize(MetricSerializationResult)}; however
         * only metrics that were fully serialized before the failure will be returned.
         *
         * @param counters counters to serialize
         * @param gauges gauges to serialize
         * @param histograms histograms to serialize
         * @return MetricSerializationResult containing the serialized metrics and the count of each
         *     metric type
         */
        public MetricSerializationResult serialize(
                Map<Counter, Tuple2<QueryScopeInfo, String>> counters,
                Map<Gauge<?>, Tuple2<QueryScopeInfo, String>> gauges,
                Map<Histogram, Tuple2<QueryScopeInfo, String>> histograms,
                Map<Meter, Tuple2<QueryScopeInfo, String>> meters) {

            countersBuffer.clear();
            int numCounters = 0;
            for (Map.Entry<Counter, Tuple2<QueryScopeInfo, String>> entry : counters.entrySet()) {
                try {
                    serializeCounter(
                            countersBuffer,
                            entry.getValue().f0,
                            entry.getValue().f1,
                            entry.getKey());
                    numCounters++;
                } catch (Exception e) {
                    LOG.debug("Failed to serialize counter.", e);
                }
            }

            gaugesBuffer.clear();
            int numGauges = 0;
            for (Map.Entry<Gauge<?>, Tuple2<QueryScopeInfo, String>> entry : gauges.entrySet()) {
                try {
                    serializeGauge(
                            gaugesBuffer, entry.getValue().f0, entry.getValue().f1, entry.getKey());
                    numGauges++;
                } catch (Exception e) {
                    LOG.debug("Failed to serialize gauge.", e);
                }
            }

            histogramsBuffer.clear();
            int numHistograms = 0;
            for (Map.Entry<Histogram, Tuple2<QueryScopeInfo, String>> entry :
                    histograms.entrySet()) {
                try {
                    serializeHistogram(
                            histogramsBuffer,
                            entry.getValue().f0,
                            entry.getValue().f1,
                            entry.getKey());
                    numHistograms++;
                } catch (Exception e) {
                    LOG.debug("Failed to serialize histogram.", e);
                }
            }

            metersBuffer.clear();
            int numMeters = 0;
            for (Map.Entry<Meter, Tuple2<QueryScopeInfo, String>> entry : meters.entrySet()) {
                try {
                    serializeMeter(
                            metersBuffer, entry.getValue().f0, entry.getValue().f1, entry.getKey());
                    numMeters++;
                } catch (Exception e) {
                    LOG.debug("Failed to serialize meter.", e);
                }
            }

            return new MetricSerializationResult(
                    countersBuffer.getCopyOfBuffer(),
                    gaugesBuffer.getCopyOfBuffer(),
                    metersBuffer.getCopyOfBuffer(),
                    histogramsBuffer.getCopyOfBuffer(),
                    numCounters,
                    numGauges,
                    numMeters,
                    numHistograms);
        }

        public void close() {
            countersBuffer = null;
            gaugesBuffer = null;
            metersBuffer = null;
            histogramsBuffer = null;
        }
    }

    private static void serializeMetricInfo(DataOutput out, QueryScopeInfo info)
            throws IOException {
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
                QueryScopeInfo.TaskQueryScopeInfo taskInfo =
                        (QueryScopeInfo.TaskQueryScopeInfo) info;
                out.writeUTF(taskInfo.jobID);
                out.writeUTF(taskInfo.vertexID);
                out.writeInt(taskInfo.subtaskIndex);
                out.writeInt(taskInfo.attemptNumber);
                break;
            case INFO_CATEGORY_OPERATOR:
                QueryScopeInfo.OperatorQueryScopeInfo operatorInfo =
                        (QueryScopeInfo.OperatorQueryScopeInfo) info;
                out.writeUTF(operatorInfo.jobID);
                out.writeUTF(operatorInfo.vertexID);
                out.writeInt(operatorInfo.subtaskIndex);
                out.writeInt(operatorInfo.attemptNumber);
                out.writeUTF(operatorInfo.operatorName);
                break;
            case INFO_CATEGORY_JM_OPERATOR:
                QueryScopeInfo.JobManagerOperatorQueryScopeInfo jmOperatorInfo =
                        (QueryScopeInfo.JobManagerOperatorQueryScopeInfo) info;
                out.writeUTF(jmOperatorInfo.jobID);
                out.writeUTF(jmOperatorInfo.vertexID);
                out.writeUTF(jmOperatorInfo.operatorName);
                break;
            default:
                throw new IOException("Unknown scope category: " + info.getCategory());
        }
    }

    private static void serializeCounter(
            DataOutput out, QueryScopeInfo info, String name, Counter counter) throws IOException {
        long count = counter.getCount();
        serializeMetricInfo(out, info);
        out.writeUTF(name);
        out.writeLong(count);
    }

    private static void serializeGauge(
            DataOutput out, QueryScopeInfo info, String name, Gauge<?> gauge) throws IOException {
        Object value = gauge.getValue();
        if (value == null) {
            throw new NullPointerException("Value returned by gauge " + name + " was null.");
        }
        String stringValue = value.toString();
        if (stringValue == null) {
            throw new NullPointerException(
                    "toString() of the value returned by gauge " + name + " returned null.");
        }

        serializeMetricInfo(out, info);
        out.writeUTF(name);
        out.writeUTF(stringValue);
    }

    private static void serializeHistogram(
            DataOutput out, QueryScopeInfo info, String name, Histogram histogram)
            throws IOException {
        HistogramStatistics stat = histogram.getStatistics();
        long min = stat.getMin();
        long max = stat.getMax();
        double mean = stat.getMean();
        double median = stat.getQuantile(0.5);
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
        out.writeDouble(median);
        out.writeDouble(stddev);
        out.writeDouble(p75);
        out.writeDouble(p90);
        out.writeDouble(p95);
        out.writeDouble(p98);
        out.writeDouble(p99);
        out.writeDouble(p999);
    }

    private static void serializeMeter(
            DataOutput out, QueryScopeInfo info, String name, Meter meter) throws IOException {
        serializeMetricInfo(out, info);
        out.writeUTF(name);
        out.writeDouble(meter.getRate());
    }

    // -------------------------------------------------------------------------
    // Deserialization
    // -------------------------------------------------------------------------

    /**
     * Deserializer for reading a list of {@link MetricDump MetricDumps} from a {@link
     * MetricSerializationResult}.
     */
    public static class MetricDumpDeserializer {
        /**
         * De-serializes metrics from the given byte array and returns them as a list of {@link
         * MetricDump}.
         *
         * @param data serialized metrics
         * @return A list containing the deserialized metrics.
         */
        public List<MetricDump> deserialize(
                MetricDumpSerialization.MetricSerializationResult data) {
            DataInputView countersInputView =
                    new DataInputDeserializer(
                            data.serializedCounters, 0, data.serializedCounters.length);
            DataInputView gaugesInputView =
                    new DataInputDeserializer(
                            data.serializedGauges, 0, data.serializedGauges.length);
            DataInputView metersInputView =
                    new DataInputDeserializer(
                            data.serializedMeters, 0, data.serializedMeters.length);
            DataInputView histogramsInputView =
                    new DataInputDeserializer(
                            data.serializedHistograms, 0, data.serializedHistograms.length);

            List<MetricDump> metrics =
                    new ArrayList<>(
                            data.numCounters
                                    + data.numGauges
                                    + data.numMeters
                                    + data.numHistograms);

            for (int x = 0; x < data.numCounters; x++) {
                try {
                    metrics.add(deserializeCounter(countersInputView));
                } catch (Exception e) {
                    LOG.debug("Failed to deserialize counter.", e);
                }
            }

            for (int x = 0; x < data.numGauges; x++) {
                try {
                    metrics.add(deserializeGauge(gaugesInputView));
                } catch (Exception e) {
                    LOG.debug("Failed to deserialize gauge.", e);
                }
            }

            for (int x = 0; x < data.numMeters; x++) {
                try {
                    metrics.add(deserializeMeter(metersInputView));
                } catch (Exception e) {
                    LOG.debug("Failed to deserialize meter.", e);
                }
            }

            for (int x = 0; x < data.numHistograms; x++) {
                try {
                    metrics.add(deserializeHistogram(histogramsInputView));
                } catch (Exception e) {
                    LOG.debug("Failed to deserialize histogram.", e);
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

    private static MetricDump.HistogramDump deserializeHistogram(DataInputView dis)
            throws IOException {
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

        return new MetricDump.HistogramDump(
                info, name, min, max, mean, median, stddev, p75, p90, p95, p98, p99, p999);
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
        int attemptNumber;
        String operatorName;

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
                attemptNumber = dis.readInt();
                return new QueryScopeInfo.TaskQueryScopeInfo(
                        jobID, vertexID, subtaskIndex, attemptNumber, scope);
            case INFO_CATEGORY_OPERATOR:
                jobID = dis.readUTF();
                vertexID = dis.readUTF();
                subtaskIndex = dis.readInt();
                attemptNumber = dis.readInt();
                operatorName = dis.readUTF();
                return new QueryScopeInfo.OperatorQueryScopeInfo(
                        jobID, vertexID, subtaskIndex, attemptNumber, operatorName, scope);
            case INFO_CATEGORY_JM_OPERATOR:
                jobID = dis.readUTF();
                vertexID = dis.readUTF();
                operatorName = dis.readUTF();
                return new QueryScopeInfo.JobManagerOperatorQueryScopeInfo(
                        jobID, vertexID, operatorName, scope);
            default:
                throw new IOException("Unknown scope category: " + cat);
        }
    }
}
