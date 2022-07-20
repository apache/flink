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

package org.apache.flink.streaming.examples.windowing.util;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.Arrays;
import java.util.Random;

/** A simple in-memory source. */
public class CarSource implements SourceFunction<Tuple4<Integer, Integer, Double, Long>> {

    private static final long serialVersionUID = 1L;
    private Integer[] speeds;
    private Double[] distances;

    private Random rand = new Random();

    private volatile boolean isRunning = true;

    private CarSource(int numOfCars) {
        speeds = new Integer[numOfCars];
        distances = new Double[numOfCars];
        Arrays.fill(speeds, 50);
        Arrays.fill(distances, 0d);
    }

    public static CarSource create(int cars) {
        return new CarSource(cars);
    }

    @Override
    public void run(SourceFunction.SourceContext<Tuple4<Integer, Integer, Double, Long>> ctx)
            throws Exception {

        while (isRunning) {
            Thread.sleep(100);
            for (int carId = 0; carId < speeds.length; carId++) {
                if (rand.nextBoolean()) {
                    speeds[carId] = Math.min(100, speeds[carId] + 5);
                } else {
                    speeds[carId] = Math.max(0, speeds[carId] - 5);
                }
                distances[carId] += speeds[carId] / 3.6d;
                Tuple4<Integer, Integer, Double, Long> record =
                        new Tuple4<>(
                                carId, speeds[carId], distances[carId], System.currentTimeMillis());
                ctx.collectWithTimestamp(record, record.f3);
            }

            ctx.emitWatermark(new Watermark(System.currentTimeMillis()));
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
