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

package org.apache.flink.table.runtime.functions.scalar;

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.GeographyData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.GeographyTypeSerializer;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.GeographyType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Benchmark-style baseline coverage for GEOGRAPHY WKB runtime hot paths.
 *
 * <p>This class is intentionally placed under {@code src/test/java}. It is not a correctness test
 * and does not enforce wall-clock assertions. Instead, it provides a runnable baseline that a
 * developer can execute directly via {@link #main(String[])} or from an IDE while CI keeps a small
 * smoke test over the wiring.
 */
public class GeographyWkbBenchmark {

    private static final int ID_POS = 0;
    private static final int NAME_POS = 1;
    private static final int ACTIVE_POS = 2;
    private static final int EVENT_TIME_POS = 3;
    private static final int GEOGRAPHY_POS = 4;

    private static final byte[] POINT_WKB =
            new byte[] {1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, -16, 63, 0, 0, 0, 0, 0, 0, 0, 64};

    private static final byte[] LINESTRING_WKB =
            new byte[] {
                1, 2, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, -16, 63, 0, 0, 0, 0, 0, 0, -16, 63
            };

    private static final RowType MIXED_ROW_TYPE =
            RowType.of(
                    new LogicalType[] {
                        new BigIntType(false),
                        new VarCharType(VarCharType.MAX_LENGTH),
                        new BooleanType(),
                        new TimestampType(3),
                        new GeographyType()
                    });

    private static final int DEFAULT_ITERATIONS = 100_000;

    private final RowDataSerializer rowDataSerializer = new RowDataSerializer(MIXED_ROW_TYPE);
    private final RowDataSerializer binaryRowAccessSerializer =
            new RowDataSerializer(MIXED_ROW_TYPE);
    private final DataOutputSerializer geographyOutput = new DataOutputSerializer(128);
    private final DataInputDeserializer geographyInput = new DataInputDeserializer();
    private final DataOutputSerializer rowOutput = new DataOutputSerializer(256);
    private final DataInputDeserializer rowInput = new DataInputDeserializer();

    private PayloadKind payloadKind;
    private byte[] geographyBytes;
    private GeographyData geography;
    private GenericRowData genericRow;
    private BinaryRowData binaryRow;

    public void setup(PayloadKind payloadKind) {
        this.payloadKind = payloadKind;
        this.geographyBytes = payloadKind.createWkb();
        this.geography = GeographyData.fromBytes(geographyBytes);
        this.genericRow = createRow(geography);
        this.binaryRow = binaryRowAccessSerializer.toBinaryRow(genericRow, true);
    }

    public PayloadKind getPayloadKind() {
        return payloadKind;
    }

    public Map<String, BenchmarkResult> runAllBenchmarks(int iterations) throws IOException {
        final LinkedHashMap<String, BenchmarkResult> results = new LinkedHashMap<>();
        results.put(
                "geographySerializerRoundTrip",
                measure(iterations, this::benchmarkSerializerRoundTrip));
        results.put("geographySerializerCopy", measure(iterations, this::benchmarkSerializerCopy));
        results.put(
                "rowDataSerializerRoundTrip",
                measure(iterations, this::benchmarkRowDataSerializerRoundTrip));
        results.put(
                "genericRowGeographyAccess",
                measure(iterations, this::benchmarkGenericRowGeographyAccess));
        results.put(
                "binaryRowGeographyAccess",
                measure(iterations, this::benchmarkBinaryRowGeographyAccess));
        results.put("stGeogFromWkb", measure(iterations, this::benchmarkStGeogFromWkb));
        results.put("stAsWkb", measure(iterations, this::benchmarkStAsWkb));
        return results;
    }

    public long benchmarkSerializerRoundTrip(int iterations) throws IOException {
        long checksum = 0L;
        for (int i = 0; i < iterations; i++) {
            geographyOutput.clear();
            GeographyTypeSerializer.INSTANCE.serialize(geography, geographyOutput);
            geographyInput.setBuffer(geographyOutput.wrapAsByteBuffer());
            checksum +=
                    GeographyTypeSerializer.INSTANCE.deserialize(geographyInput).toBytes().length;
        }
        return checksum;
    }

    public long benchmarkSerializerCopy(int iterations) {
        long checksum = 0L;
        for (int i = 0; i < iterations; i++) {
            checksum += GeographyTypeSerializer.INSTANCE.copy(geography).toBytes().length;
        }
        return checksum;
    }

    public long benchmarkRowDataSerializerRoundTrip(int iterations) throws IOException {
        long checksum = 0L;
        for (int i = 0; i < iterations; i++) {
            rowOutput.clear();
            rowDataSerializer.serialize(genericRow, rowOutput);
            rowInput.setBuffer(rowOutput.wrapAsByteBuffer());
            final RowData deserialized = rowDataSerializer.deserialize(rowInput);
            checksum += deserialized.getLong(ID_POS);
            checksum += deserialized.getString(NAME_POS).toBytes().length;
            checksum += deserialized.getBoolean(ACTIVE_POS) ? 1 : 0;
            checksum += deserialized.getTimestamp(EVENT_TIME_POS, 3).getMillisecond();
            checksum += deserialized.getGeography(GEOGRAPHY_POS).toBytes().length;
        }
        return checksum;
    }

    public long benchmarkGenericRowGeographyAccess(int iterations) {
        long checksum = 0L;
        for (int i = 0; i < iterations; i++) {
            checksum += genericRow.getGeography(GEOGRAPHY_POS).toBytes().length;
        }
        return checksum;
    }

    public long benchmarkBinaryRowGeographyAccess(int iterations) {
        long checksum = 0L;
        for (int i = 0; i < iterations; i++) {
            checksum += binaryRow.getGeography(GEOGRAPHY_POS).toBytes().length;
        }
        return checksum;
    }

    public long benchmarkStGeogFromWkb(int iterations) {
        long checksum = 0L;
        for (int i = 0; i < iterations; i++) {
            final GeographyData converted = GeographyConversionUtils.fromWkb(geographyBytes);
            checksum += converted.subtypeId();
            checksum += converted.toBytes().length;
        }
        return checksum;
    }

    public long benchmarkStAsWkb(int iterations) {
        long checksum = 0L;
        for (int i = 0; i < iterations; i++) {
            checksum += GeographyConversionUtils.asWkb(geography).length;
        }
        return checksum;
    }

    public static void main(String[] args) throws IOException {
        final int iterations = args.length > 0 ? Integer.parseInt(args[0]) : DEFAULT_ITERATIONS;

        for (PayloadKind payloadKind : PayloadKind.values()) {
            final GeographyWkbBenchmark benchmark = new GeographyWkbBenchmark();
            benchmark.setup(payloadKind);

            System.out.printf("Payload: %s (%d iterations)%n", payloadKind.displayName, iterations);
            for (Map.Entry<String, BenchmarkResult> entry :
                    benchmark.runAllBenchmarks(iterations).entrySet()) {
                final BenchmarkResult result = entry.getValue();
                System.out.printf(
                        "  %-30s %8d ns/op checksum=%d%n",
                        entry.getKey(), result.getNanosPerOperation(), result.getChecksum());
            }
        }
    }

    private BenchmarkResult measure(int iterations, CheckedBenchmark benchmark) throws IOException {
        benchmark.run(Math.max(1, Math.min(iterations, 256)));

        final long startNanos = System.nanoTime();
        final long checksum = benchmark.run(iterations);
        final long elapsedNanos = System.nanoTime() - startNanos;
        return new BenchmarkResult(elapsedNanos / Math.max(1, iterations), checksum);
    }

    private static GenericRowData createRow(GeographyData geography) {
        final GenericRowData row = new GenericRowData(5);
        row.setField(ID_POS, 42L);
        row.setField(NAME_POS, StringData.fromString("benchmark-geography"));
        row.setField(ACTIVE_POS, true);
        row.setField(
                EVENT_TIME_POS,
                TimestampData.fromLocalDateTime(
                        LocalDateTime.of(2025, 1, 2, 3, 4, 5, 123_000_000)));
        row.setField(GEOGRAPHY_POS, geography);
        return row;
    }

    private interface CheckedBenchmark {
        long run(int iterations) throws IOException;
    }

    /** Representative payloads for serializer and conversion hot paths. */
    public enum PayloadKind {
        SMALL_POINT("small-point") {
            @Override
            byte[] createWkb() {
                return POINT_WKB;
            }
        },
        EMPTY_COLLECTION("empty-geometry-collection") {
            @Override
            byte[] createWkb() {
                return GeographyTypeSerializer.INSTANCE.createInstance().toBytes();
            }
        },
        LARGE_LINESTRING("large-linestring") {
            @Override
            byte[] createWkb() {
                return LINESTRING_WKB;
            }
        };

        private final String displayName;

        PayloadKind(String displayName) {
            this.displayName = displayName;
        }

        abstract byte[] createWkb();
    }

    /** Immutable benchmark result for one measured hot path. */
    public static final class BenchmarkResult {
        private final long nanosPerOperation;
        private final long checksum;

        private BenchmarkResult(long nanosPerOperation, long checksum) {
            this.nanosPerOperation = nanosPerOperation;
            this.checksum = checksum;
        }

        public long getNanosPerOperation() {
            return nanosPerOperation;
        }

        public long getChecksum() {
            return checksum;
        }
    }
}
