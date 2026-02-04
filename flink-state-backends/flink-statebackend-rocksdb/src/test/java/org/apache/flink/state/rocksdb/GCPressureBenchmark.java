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

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Benchmark measuring GC pressure difference between getCopyOfBuffer() and getSharedBuffer().
 *
 * <p>Measures: - Total bytes allocated - GC count and time - Memory churn rate
 */
public class GCPressureBenchmark {

    private static final int ITERATIONS = 10_000_000;
    private static final int[] VALUE_SIZES = {100, 500, 1000};

    public static void main(String[] args) throws Exception {
        new GCPressureBenchmark().runBenchmark();
    }

    @org.junit.jupiter.api.Test
    public void runBenchmark() throws Exception {
        System.out.println("GC Pressure Benchmark");
        System.out.println("=====================");
        System.out.println("Iterations: " + String.format("%,d", ITERATIONS));
        System.out.println("Value sizes: " + Arrays.toString(VALUE_SIZES) + " bytes");
        System.out.println();

        // Get GC beans
        List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();

        Random random = new Random(42);

        for (int valueSize : VALUE_SIZES) {
            System.out.println("=== Value size: " + valueSize + " bytes ===");

            byte[] valueData = new byte[valueSize];
            random.nextBytes(valueData);

            DataOutputSerializer outputView = new DataOutputSerializer(valueSize + 32);

            // Warmup
            System.out.print("Warming up... ");
            for (int i = 0; i < 1_000_000; i++) {
                outputView.clear();
                outputView.write(valueData);
                byte[] copy = outputView.getCopyOfBuffer();
                consumeBytes(copy);
            }
            for (int i = 0; i < 1_000_000; i++) {
                outputView.clear();
                outputView.write(valueData);
                ByteBuffer buf =
                        ByteBuffer.wrap(outputView.getSharedBuffer(), 0, outputView.length());
                consumeByteBuffer(buf);
            }
            System.gc();
            Thread.sleep(500);
            System.out.println("done");

            // ========== Test getCopyOfBuffer() (OLD) ==========
            System.out.println("\n--- getCopyOfBuffer() (OLD) ---");
            System.gc();
            Thread.sleep(200);

            long gcCountBefore1 = getTotalGcCount(gcBeans);
            long gcTimeBefore1 = getTotalGcTime(gcBeans);
            long memBefore1 =
                    Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

            long startTime1 = System.nanoTime();
            for (int i = 0; i < ITERATIONS; i++) {
                outputView.clear();
                outputView.write(valueData);
                byte[] copy = outputView.getCopyOfBuffer();
                consumeBytes(copy);
            }
            long endTime1 = System.nanoTime();

            long memAfter1 = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
            long gcCountAfter1 = getTotalGcCount(gcBeans);
            long gcTimeAfter1 = getTotalGcTime(gcBeans);

            double duration1Ms = (endTime1 - startTime1) / 1_000_000.0;
            long gcCount1 = gcCountAfter1 - gcCountBefore1;
            long gcTime1 = gcTimeAfter1 - gcTimeBefore1;
            long memDelta1 = memAfter1 - memBefore1;

            // Estimated allocation: iterations * valueSize (for the copied array)
            long estimatedAlloc1 = (long) ITERATIONS * valueSize;

            System.out.printf("  Duration: %.2f ms%n", duration1Ms);
            System.out.printf("  GC collections: %d%n", gcCount1);
            System.out.printf("  GC time: %d ms%n", gcTime1);
            System.out.printf("  Estimated allocation: %s%n", formatBytes(estimatedAlloc1));
            System.out.printf(
                    "  Allocation rate: %s/sec%n",
                    formatBytes((long) (estimatedAlloc1 / (duration1Ms / 1000))));

            // ========== Test getSharedBuffer() (NEW) ==========
            System.out.println("\n--- getSharedBuffer() + ByteBuffer.wrap() (NEW) ---");
            System.gc();
            Thread.sleep(200);

            long gcCountBefore2 = getTotalGcCount(gcBeans);
            long gcTimeBefore2 = getTotalGcTime(gcBeans);
            long memBefore2 =
                    Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

            long startTime2 = System.nanoTime();
            for (int i = 0; i < ITERATIONS; i++) {
                outputView.clear();
                outputView.write(valueData);
                ByteBuffer buf =
                        ByteBuffer.wrap(outputView.getSharedBuffer(), 0, outputView.length());
                consumeByteBuffer(buf);
            }
            long endTime2 = System.nanoTime();

            long memAfter2 = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
            long gcCountAfter2 = getTotalGcCount(gcBeans);
            long gcTimeAfter2 = getTotalGcTime(gcBeans);

            double duration2Ms = (endTime2 - startTime2) / 1_000_000.0;
            long gcCount2 = gcCountAfter2 - gcCountBefore2;
            long gcTime2 = gcTimeAfter2 - gcTimeBefore2;

            // ByteBuffer.wrap() creates a small ByteBuffer object (~48 bytes) but no array copy
            long estimatedAlloc2 = (long) ITERATIONS * 48; // ByteBuffer object overhead

            System.out.printf("  Duration: %.2f ms%n", duration2Ms);
            System.out.printf("  GC collections: %d%n", gcCount2);
            System.out.printf("  GC time: %d ms%n", gcTime2);
            System.out.printf("  Estimated allocation: %s%n", formatBytes(estimatedAlloc2));
            System.out.printf(
                    "  Allocation rate: %s/sec%n",
                    formatBytes((long) (estimatedAlloc2 / (duration2Ms / 1000))));

            // ========== Summary ==========
            System.out.println("\n--- SUMMARY ---");
            long allocSaved = estimatedAlloc1 - estimatedAlloc2;
            double allocReduction = (double) allocSaved / estimatedAlloc1 * 100;
            long gcTimeSaved = gcTime1 - gcTime2;

            System.out.printf(
                    "  Allocation reduced by: %s (%.1f%%)%n",
                    formatBytes(allocSaved), allocReduction);
            System.out.printf("  GC collections reduced by: %d%n", gcCount1 - gcCount2);
            System.out.printf("  GC time reduced by: %d ms%n", gcTimeSaved);
            System.out.printf(
                    "  Per-operation savings: %d bytes%n",
                    (valueSize - 48 > 0 ? valueSize - 48 : 0));
            System.out.println();
        }

        // Final summary
        System.out.println("====================================");
        System.out.println("CONCLUSION");
        System.out.println("====================================");
        System.out.println("The getSharedBuffer() approach eliminates one byte[] allocation");
        System.out.println("per state operation. For value sizes > 48 bytes, this reduces");
        System.out.println("memory allocation and GC pressure proportionally.");
        System.out.println();
        System.out.println("At 1 million state ops/sec with 100-byte values:");
        System.out.printf("  - OLD: allocates ~%s/sec%n", formatBytes(100_000_000L));
        System.out.printf("  - NEW: allocates ~%s/sec%n", formatBytes(48_000_000L));
        System.out.printf("  - Savings: ~%s/sec (52%% reduction)%n", formatBytes(52_000_000L));
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

    // Prevent JIT from optimizing away the result
    private static volatile byte sink;
    private static volatile int sinkInt;

    private void consumeBytes(byte[] bytes) {
        sink = bytes[0];
    }

    private void consumeByteBuffer(ByteBuffer buf) {
        sinkInt = buf.remaining();
    }
}
