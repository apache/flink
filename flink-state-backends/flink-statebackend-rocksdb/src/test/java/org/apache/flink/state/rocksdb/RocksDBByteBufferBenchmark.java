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

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Micro-benchmark comparing byte[] vs ByteBuffer serialization for RocksDB put operations.
 *
 * <p>Run with: mvn test -pl flink-state-backends/flink-statebackend-rocksdb \
 * -Dtest=RocksDBByteBufferBenchmark#runBenchmark -q
 */
public class RocksDBByteBufferBenchmark {

    private static final int WARMUP_ITERATIONS = 100_000;
    private static final int BENCHMARK_ITERATIONS = 200_000;
    private static final int NUM_ROUNDS = 5;
    private static final int[] VALUE_SIZES = {50, 100, 500, 1000};

    public static void main(String[] args) throws Exception {
        new RocksDBByteBufferBenchmark().runBenchmark();
    }

    @org.junit.jupiter.api.Test
    public void runBenchmark() throws Exception {
        RocksDB.loadLibrary();

        Path tempDir = Files.createTempDirectory("rocksdb-benchmark");
        System.out.println("RocksDB ByteBuffer vs byte[] Benchmark");
        System.out.println("======================================");
        System.out.println("Warmup iterations per round: " + WARMUP_ITERATIONS);
        System.out.println("Benchmark iterations per round: " + BENCHMARK_ITERATIONS);
        System.out.println("Number of rounds: " + NUM_ROUNDS);
        System.out.println("Value sizes: " + Arrays.toString(VALUE_SIZES) + " bytes");
        System.out.println();

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

                for (int valueSize : VALUE_SIZES) {
                    System.out.println("--- Value size: " + valueSize + " bytes ---");

                    byte[] valueData = new byte[valueSize];
                    random.nextBytes(valueData);

                    DataOutputSerializer outputView = new DataOutputSerializer(valueSize + 32);

                    double[] byteArrayTimes = new double[NUM_ROUNDS];
                    double[] byteBufferTimes = new double[NUM_ROUNDS];

                    // Warmup both approaches
                    System.out.print("Warming up... ");
                    int keyOffset = 0;
                    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
                        byte[] key = serializeKey(keyOffset++);
                        outputView.clear();
                        outputView.write(valueData);
                        byte[] value = outputView.getCopyOfBuffer();
                        db.put(cf, writeOptions, key, value);
                    }
                    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
                        byte[] key = serializeKey(keyOffset++);
                        outputView.clear();
                        outputView.write(valueData);
                        ByteBuffer keyBuf = ByteBuffer.wrap(key);
                        ByteBuffer valueBuf =
                                ByteBuffer.wrap(
                                        outputView.getSharedBuffer(), 0, outputView.length());
                        db.put(cf, writeOptions, keyBuf, valueBuf);
                    }
                    db.compactRange();
                    System.out.println("done");

                    // Run alternating rounds to reduce ordering bias
                    for (int round = 0; round < NUM_ROUNDS; round++) {
                        System.out.print("Round " + (round + 1) + "... ");

                        // byte[] approach
                        long startByteArray = System.nanoTime();
                        for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
                            byte[] key = serializeKey(keyOffset++);
                            outputView.clear();
                            outputView.write(valueData);
                            byte[] value = outputView.getCopyOfBuffer();
                            db.put(cf, writeOptions, key, value);
                        }
                        long endByteArray = System.nanoTime();
                        byteArrayTimes[round] =
                                (endByteArray - startByteArray) / (double) BENCHMARK_ITERATIONS;

                        // ByteBuffer approach
                        long startByteBuffer = System.nanoTime();
                        for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
                            byte[] key = serializeKey(keyOffset++);
                            outputView.clear();
                            outputView.write(valueData);
                            ByteBuffer keyBuf = ByteBuffer.wrap(key);
                            ByteBuffer valueBuf =
                                    ByteBuffer.wrap(
                                            outputView.getSharedBuffer(), 0, outputView.length());
                            db.put(cf, writeOptions, keyBuf, valueBuf);
                        }
                        long endByteBuffer = System.nanoTime();
                        byteBufferTimes[round] =
                                (endByteBuffer - startByteBuffer) / (double) BENCHMARK_ITERATIONS;

                        System.out.printf(
                                "byte[]=%.1f ns, ByteBuffer=%.1f ns%n",
                                byteArrayTimes[round], byteBufferTimes[round]);

                        db.compactRange();
                    }

                    // Calculate averages (excluding first round for additional warmup)
                    double avgByteArray = 0, avgByteBuffer = 0;
                    for (int i = 1; i < NUM_ROUNDS; i++) {
                        avgByteArray += byteArrayTimes[i];
                        avgByteBuffer += byteBufferTimes[i];
                    }
                    avgByteArray /= (NUM_ROUNDS - 1);
                    avgByteBuffer /= (NUM_ROUNDS - 1);

                    double improvement = ((avgByteArray - avgByteBuffer) / avgByteArray) * 100;

                    System.out.printf(
                            "RESULT [%d bytes]: byte[]=%.2f ns, ByteBuffer=%.2f ns, ",
                            valueSize, avgByteArray, avgByteBuffer);
                    if (improvement > 0) {
                        System.out.printf("Improvement: %.2f%%%n", improvement);
                    } else {
                        System.out.printf("Regression: %.2f%%%n", -improvement);
                    }
                    System.out.println();
                }

                cf.close();
            }
        } finally {
            deleteDirectory(tempDir.toFile());
        }
    }

    private byte[] serializeKey(int i) {
        // Simulate a typical Flink key: keyGroup (2 bytes) + key (4 bytes) + namespace (4 bytes)
        ByteBuffer buf = ByteBuffer.allocate(10);
        buf.putShort((short) (i % 128));
        buf.putInt(i);
        buf.putInt(0);
        return buf.array();
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
}
