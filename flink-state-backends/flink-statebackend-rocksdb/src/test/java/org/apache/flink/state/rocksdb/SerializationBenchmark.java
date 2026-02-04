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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

/**
 * Micro-benchmark comparing getCopyOfBuffer() vs getSharedBuffer() serialization overhead.
 *
 * <p>This isolates the serialization overhead without RocksDB I/O noise.
 */
public class SerializationBenchmark {

    private static final int WARMUP_ITERATIONS = 500_000;
    private static final int BENCHMARK_ITERATIONS = 5_000_000;
    private static final int NUM_ROUNDS = 5;
    private static final int[] VALUE_SIZES = {50, 100, 500, 1000};

    public static void main(String[] args) throws Exception {
        new SerializationBenchmark().runBenchmark();
    }

    @org.junit.jupiter.api.Test
    public void runBenchmark() throws Exception {
        System.out.println("Serialization Overhead Benchmark");
        System.out.println("================================");
        System.out.println("(Isolates getCopyOfBuffer() vs getSharedBuffer() + ByteBuffer.wrap())");
        System.out.println("Warmup iterations: " + WARMUP_ITERATIONS);
        System.out.println("Benchmark iterations: " + BENCHMARK_ITERATIONS);
        System.out.println("Number of rounds: " + NUM_ROUNDS);
        System.out.println("Value sizes: " + Arrays.toString(VALUE_SIZES) + " bytes");
        System.out.println();

        Random random = new Random(42);

        for (int valueSize : VALUE_SIZES) {
            System.out.println("--- Value size: " + valueSize + " bytes ---");

            byte[] valueData = new byte[valueSize];
            random.nextBytes(valueData);

            DataOutputSerializer outputView = new DataOutputSerializer(valueSize + 32);

            double[] copyTimes = new double[NUM_ROUNDS];
            double[] sharedTimes = new double[NUM_ROUNDS];

            // Warmup
            System.out.print("Warming up... ");
            for (int i = 0; i < WARMUP_ITERATIONS; i++) {
                outputView.clear();
                outputView.write(valueData);
                byte[] copy = outputView.getCopyOfBuffer();
                consumeBytes(copy);
            }
            for (int i = 0; i < WARMUP_ITERATIONS; i++) {
                outputView.clear();
                outputView.write(valueData);
                ByteBuffer buf =
                        ByteBuffer.wrap(outputView.getSharedBuffer(), 0, outputView.length());
                consumeByteBuffer(buf);
            }
            System.out.println("done");

            // Run alternating rounds
            for (int round = 0; round < NUM_ROUNDS; round++) {
                System.out.print("Round " + (round + 1) + "... ");

                // getCopyOfBuffer() approach (OLD)
                long startCopy = System.nanoTime();
                for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
                    outputView.clear();
                    outputView.write(valueData);
                    byte[] copy = outputView.getCopyOfBuffer();
                    consumeBytes(copy);
                }
                long endCopy = System.nanoTime();
                copyTimes[round] = (endCopy - startCopy) / (double) BENCHMARK_ITERATIONS;

                // getSharedBuffer() + ByteBuffer.wrap() approach (NEW)
                long startShared = System.nanoTime();
                for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
                    outputView.clear();
                    outputView.write(valueData);
                    ByteBuffer buf =
                            ByteBuffer.wrap(outputView.getSharedBuffer(), 0, outputView.length());
                    consumeByteBuffer(buf);
                }
                long endShared = System.nanoTime();
                sharedTimes[round] = (endShared - startShared) / (double) BENCHMARK_ITERATIONS;

                System.out.printf(
                        "getCopyOfBuffer()=%.2f ns, getSharedBuffer()=%.2f ns%n",
                        copyTimes[round], sharedTimes[round]);
            }

            // Calculate averages (excluding first round)
            double avgCopy = 0, avgShared = 0;
            for (int i = 1; i < NUM_ROUNDS; i++) {
                avgCopy += copyTimes[i];
                avgShared += sharedTimes[i];
            }
            avgCopy /= (NUM_ROUNDS - 1);
            avgShared /= (NUM_ROUNDS - 1);

            double improvement = ((avgCopy - avgShared) / avgCopy) * 100;

            System.out.printf(
                    "RESULT [%d bytes]: getCopyOfBuffer()=%.2f ns, getSharedBuffer()=%.2f ns, ",
                    valueSize, avgCopy, avgShared);
            if (improvement > 0) {
                System.out.printf("Improvement: %.2f%%%n", improvement);
            } else {
                System.out.printf("Regression: %.2f%%%n", -improvement);
            }
            System.out.println();
        }
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
