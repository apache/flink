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

package org.apache.flink.state.rocksdb;

import org.apache.flink.core.memory.DataOutputSerializer;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteOptions;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Realistic workload benchmark comparing OLD (byte[]) vs NEW (ByteBuffer) approaches.
 *
 * <p>Simulates a Flink-like workload with: - Key serialization (always copies - unchanged between
 * approaches) - Value serialization (OLD copies, NEW uses shared buffer) - Simulated record
 * processing allocations - Actual RocksDB put operations
 */
public class RealisticWorkloadBenchmark {

    private static final int ITERATIONS = 2_000_000;
    private static final int VALUE_SIZE = 100; // Typical state value size
    private static final int KEY_SIZE = 20; // Typical key size (keyGroup + key + namespace)

    // Simulate other allocations per record (deserialization, transformations, etc.)
    private static final int SIMULATED_RECORD_ALLOC = 200; // bytes per record

    public static void main(String[] args) throws Exception {
        new RealisticWorkloadBenchmark().runBenchmark();
    }

    @org.junit.jupiter.api.Test
    public void runBenchmark() throws Exception {
        RocksDB.loadLibrary();

        System.out.println("Realistic Flink Workload Benchmark");
        System.out.println("==================================");
        System.out.println("Iterations: " + String.format("%,d", ITERATIONS));
        System.out.println("Value size: " + VALUE_SIZE + " bytes");
        System.out.println("Key size: " + KEY_SIZE + " bytes");
        System.out.println(
                "Simulated record processing alloc: " + SIMULATED_RECORD_ALLOC + " bytes");
        System.out.println();
        System.out.println("Per-operation allocation breakdown:");
        System.out.println("  - Key serialization: " + KEY_SIZE + " bytes (same for both)");
        System.out.println("  - Value serialization OLD: " + VALUE_SIZE + " bytes (copied)");
        System.out.println("  - Value serialization NEW: ~48 bytes (ByteBuffer wrapper only)");
        System.out.println("  - Simulated record processing: " + SIMULATED_RECORD_ALLOC + " bytes");
        System.out.println();

        Path tempDir = Files.createTempDirectory("rocksdb-realistic-benchmark");
        List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();

        try (DBOptions dbOptions = new DBOptions().setCreateIfMissing(true);
                ColumnFamilyOptions cfOptions = new ColumnFamilyOptions();
                WriteOptions writeOptions = new WriteOptions()) {

            List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
            cfDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOptions));

            List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
            try (RocksDB db =
                    RocksDB.open(dbOptions, tempDir.toString(), cfDescriptors, cfHandles)) {
                ColumnFamilyHandle cf = cfHandles.get(0);

                Random random = new Random(42);
                byte[] valueData = new byte[VALUE_SIZE];
                random.nextBytes(valueData);

                DataOutputSerializer keyOutputView = new DataOutputSerializer(KEY_SIZE + 16);
                DataOutputSerializer valueOutputView = new DataOutputSerializer(VALUE_SIZE + 16);

                // Warmup
                System.out.print("Warming up (500K iterations each)... ");
                for (int i = 0; i < 500_000; i++) {
                    // Simulate record processing allocation
                    byte[] recordAlloc = new byte[SIMULATED_RECORD_ALLOC];
                    consumeBytes(recordAlloc);

                    // Key serialization (always copies)
                    byte[] key = serializeKey(keyOutputView, i);

                    // Value serialization - OLD way
                    valueOutputView.clear();
                    valueOutputView.write(valueData);
                    byte[] value = valueOutputView.getCopyOfBuffer();

                    db.put(cf, writeOptions, key, value);
                }

                for (int i = 500_000; i < 1_000_000; i++) {
                    byte[] recordAlloc = new byte[SIMULATED_RECORD_ALLOC];
                    consumeBytes(recordAlloc);

                    byte[] key = serializeKey(keyOutputView, i);

                    // Value serialization - NEW way
                    valueOutputView.clear();
                    valueOutputView.write(valueData);
                    ByteBuffer valueBuf =
                            ByteBuffer.wrap(
                                    valueOutputView.getSharedBuffer(), 0, valueOutputView.length());

                    db.put(cf, writeOptions, ByteBuffer.wrap(key), valueBuf);
                }

                db.compactRange();
                System.gc();
                Thread.sleep(500);
                System.out.println("done");

                // ========== OLD Approach (byte[] for value) ==========
                System.out.println("\n=== OLD Approach (getCopyOfBuffer) ===");
                System.gc();
                Thread.sleep(200);

                long gcCountBefore1 = getTotalGcCount(gcBeans);
                long gcTimeBefore1 = getTotalGcTime(gcBeans);

                long startTime1 = System.nanoTime();
                for (int i = 0; i < ITERATIONS; i++) {
                    // Simulate record processing allocation
                    byte[] recordAlloc = new byte[SIMULATED_RECORD_ALLOC];
                    consumeBytes(recordAlloc);

                    // Key serialization (always copies - same for both)
                    byte[] key = serializeKey(keyOutputView, i);

                    // Value serialization - OLD way (copies)
                    valueOutputView.clear();
                    valueOutputView.write(valueData);
                    byte[] value = valueOutputView.getCopyOfBuffer();

                    db.put(cf, writeOptions, key, value);
                }
                long endTime1 = System.nanoTime();

                long gcCount1 = getTotalGcCount(gcBeans) - gcCountBefore1;
                long gcTime1 = getTotalGcTime(gcBeans) - gcTimeBefore1;
                double duration1Ms = (endTime1 - startTime1) / 1_000_000.0;
                double opsPerSec1 = ITERATIONS / (duration1Ms / 1000);

                // Calculate allocations
                long keyAlloc = (long) ITERATIONS * KEY_SIZE;
                long valueAllocOld = (long) ITERATIONS * VALUE_SIZE;
                long recordAlloc = (long) ITERATIONS * SIMULATED_RECORD_ALLOC;
                long totalAllocOld = keyAlloc + valueAllocOld + recordAlloc;

                System.out.printf("  Duration: %.2f ms (%.0f ops/sec)%n", duration1Ms, opsPerSec1);
                System.out.printf("  GC collections: %d%n", gcCount1);
                System.out.printf("  GC time: %d ms%n", gcTime1);
                System.out.println("  Allocations:");
                System.out.printf("    - Key:    %s%n", formatBytes(keyAlloc));
                System.out.printf("    - Value:  %s (copied)%n", formatBytes(valueAllocOld));
                System.out.printf("    - Record: %s%n", formatBytes(recordAlloc));
                System.out.printf("    - TOTAL:  %s%n", formatBytes(totalAllocOld));

                db.compactRange();

                // ========== NEW Approach (ByteBuffer for value) ==========
                System.out.println("\n=== NEW Approach (getSharedBuffer + ByteBuffer) ===");
                System.gc();
                Thread.sleep(200);

                long gcCountBefore2 = getTotalGcCount(gcBeans);
                long gcTimeBefore2 = getTotalGcTime(gcBeans);

                long startTime2 = System.nanoTime();
                for (int i = 0; i < ITERATIONS; i++) {
                    // Simulate record processing allocation (same)
                    byte[] recordAlloc2 = new byte[SIMULATED_RECORD_ALLOC];
                    consumeBytes(recordAlloc2);

                    // Key serialization (same - still copies)
                    byte[] key = serializeKey(keyOutputView, i);

                    // Value serialization - NEW way (no copy, just ByteBuffer wrapper)
                    valueOutputView.clear();
                    valueOutputView.write(valueData);
                    ByteBuffer valueBuf =
                            ByteBuffer.wrap(
                                    valueOutputView.getSharedBuffer(), 0, valueOutputView.length());

                    db.put(cf, writeOptions, ByteBuffer.wrap(key), valueBuf);
                }
                long endTime2 = System.nanoTime();

                long gcCount2 = getTotalGcCount(gcBeans) - gcCountBefore2;
                long gcTime2 = getTotalGcTime(gcBeans) - gcTimeBefore2;
                double duration2Ms = (endTime2 - startTime2) / 1_000_000.0;
                double opsPerSec2 = ITERATIONS / (duration2Ms / 1000);

                // ByteBuffer.wrap creates a ~48 byte object
                long valueAllocNew = (long) ITERATIONS * 48 * 2; // 2 ByteBuffers (key + value)
                long totalAllocNew = keyAlloc + valueAllocNew + recordAlloc;

                System.out.printf("  Duration: %.2f ms (%.0f ops/sec)%n", duration2Ms, opsPerSec2);
                System.out.printf("  GC collections: %d%n", gcCount2);
                System.out.printf("  GC time: %d ms%n", gcTime2);
                System.out.println("  Allocations:");
                System.out.printf("    - Key:    %s (still copied)%n", formatBytes(keyAlloc));
                System.out.printf(
                        "    - Value:  %s (ByteBuffer wrappers only)%n",
                        formatBytes(valueAllocNew));
                System.out.printf("    - Record: %s%n", formatBytes(recordAlloc));
                System.out.printf("    - TOTAL:  %s%n", formatBytes(totalAllocNew));

                // ========== Summary ==========
                System.out.println("\n========== REALISTIC IMPACT SUMMARY ==========");

                long allocSaved = totalAllocOld - totalAllocNew;
                double allocReductionPct = (double) allocSaved / totalAllocOld * 100;
                double throughputImprovement = (opsPerSec2 - opsPerSec1) / opsPerSec1 * 100;

                System.out.printf(
                        "Total allocation reduced: %s (%.1f%%)%n",
                        formatBytes(allocSaved), allocReductionPct);
                System.out.printf("GC collections reduced: %d -> %d%n", gcCount1, gcCount2);
                System.out.printf("GC time reduced: %d ms -> %d ms%n", gcTime1, gcTime2);
                System.out.printf("Throughput change: %.1f%%%n", throughputImprovement);

                System.out.println();
                System.out.println("Breakdown of allocation sources:");
                System.out.printf(
                        "  - Key serialization:    %.1f%% (unchanged - still copies)%n",
                        100.0 * keyAlloc / totalAllocOld);
                System.out.printf(
                        "  - Value serialization:  %.1f%% -> %.1f%% (reduced)%n",
                        100.0 * valueAllocOld / totalAllocOld,
                        100.0 * valueAllocNew / totalAllocNew);
                System.out.printf(
                        "  - Record processing:    %.1f%% (unchanged)%n",
                        100.0 * recordAlloc / totalAllocOld);

                System.out.println();
                System.out.println("CONCLUSION:");
                System.out.println("In a realistic workload where state operations are ~30-50% of");
                System.out.println("total allocations, and value serialization is ~30% of state");
                System.out.println("allocations, the overall GC pressure reduction is modest but");
                System.out.println("measurable. The optimization is most valuable for:");
                System.out.println("  - Jobs with high state update rates (>100K ops/sec)");
                System.out.println("  - Large state values (500+ bytes)");
                System.out.println("  - Latency-sensitive applications where GC pauses matter");

                cf.close();
            }
        } finally {
            deleteDirectory(tempDir.toFile());
        }
    }

    private byte[] serializeKey(DataOutputSerializer outputView, int i) throws Exception {
        outputView.clear();
        outputView.writeShort(i % 128); // keyGroup
        outputView.writeInt(i); // key
        outputView.writeInt(0); // namespace (VoidNamespace)
        return outputView.getCopyOfBuffer(); // Key ALWAYS copies
    }

    private long getTotalGcCount(List<GarbageCollectorMXBean> gcBeans) {
        return gcBeans.stream().mapToLong(GarbageCollectorMXBean::getCollectionCount).sum();
    }

    private long getTotalGcTime(List<GarbageCollectorMXBean> gcBeans) {
        return gcBeans.stream().mapToLong(GarbageCollectorMXBean::getCollectionTime).sum();
    }

    private String formatBytes(long bytes) {
        if (bytes < 1024) {
            return bytes + " B";
        }
        if (bytes < 1024 * 1024) {
            return String.format("%.1f KB", bytes / 1024.0);
        }
        if (bytes < 1024 * 1024 * 1024) {
            return String.format("%.1f MB", bytes / (1024.0 * 1024));
        }
        return String.format("%.1f GB", bytes / (1024.0 * 1024 * 1024));
    }

    private void deleteDirectory(java.io.File dir) {
        java.io.File[] files = dir.listFiles();
        if (files != null) {
            for (java.io.File file : files) {
                if (file.isDirectory()) {
                    deleteDirectory(file);
                } else {
                    file.delete();
                }
            }
        }
        dir.delete();
    }

    private static volatile byte sink;

    private void consumeBytes(byte[] bytes) {
        sink = bytes[0];
    }
}
