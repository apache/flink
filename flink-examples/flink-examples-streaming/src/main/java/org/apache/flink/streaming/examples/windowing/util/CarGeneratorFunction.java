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
import org.apache.flink.connector.datagen.source.GeneratorFunction;

import java.util.Arrays;
import java.util.Random;

/**
 * A generator function for simulating car data.
 *
 * <p>This generator function generates a stream of car data in a form of a four-element tuple. The
 * data includes the car's ID, its speed in kilometers per hour, the distance it has traveled in
 * meters, and the timestamp of the data generation. The speed and distance of each car are randomly
 * updated in each invocation of the {@link #map(Long)} method.
 */
public class CarGeneratorFunction
        implements GeneratorFunction<Long, Tuple4<Integer, Integer, Double, Long>> {

    private static final long serialVersionUID = 1L;
    // in kilometers per hour
    private final int[] speeds;
    // in meters
    private final double[] distances;
    // in milliseconds
    private final long[] lastUpdate;
    private int nextCar;

    private static final int MILLIS_IN_HOUR = 1000 * 60 * 60;
    private static final double HOURS_IN_MILLI = 1d / MILLIS_IN_HOUR;
    private static final int METERS_IN_KILOMETER = 1000;

    private final Random rand = new Random();

    // Previous version (CarSource) was overestimating the speed. This factor is used to preserve
    // the original behaviour of the example.
    private static final int COMPAT_FACTOR = 10;

    public CarGeneratorFunction(int numOfCars) {
        speeds = new int[numOfCars];
        distances = new double[numOfCars];
        lastUpdate = new long[numOfCars];
        Arrays.fill(speeds, 50);
        Arrays.fill(distances, 0d);
        Arrays.fill(lastUpdate, 0);
    }

    @Override
    public Tuple4<Integer, Integer, Double, Long> map(Long ignoredIndex) throws Exception {
        if (rand.nextBoolean()) {
            speeds[nextCar] = Math.min(100, speeds[nextCar] + 5);
        } else {
            speeds[nextCar] = Math.max(0, speeds[nextCar] - 5);
        }
        long now = System.currentTimeMillis();
        long timeDiffMillis = lastUpdate[nextCar] == 0 ? 0 : now - lastUpdate[nextCar];
        lastUpdate[nextCar] = now;
        distances[nextCar] +=
                speeds[nextCar]
                        * (timeDiffMillis * HOURS_IN_MILLI)
                        * METERS_IN_KILOMETER
                        * COMPAT_FACTOR;
        nextCar = (++nextCar) % speeds.length;
        return new Tuple4<>(nextCar, speeds[nextCar], distances[nextCar], now);
    }
}
