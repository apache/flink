/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.flink.streaming.examples.windowing;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.deltafunction.DeltaFunction;
import org.apache.flink.streaming.api.windowing.helper.Delta;
import org.apache.flink.streaming.api.windowing.helper.Time;
import org.apache.flink.streaming.api.windowing.helper.Timestamp;

import java.util.Arrays;
import java.util.Random;

/**
* An example of grouped stream windowing where different eviction and trigger
* policies can be used. A source fetches events from cars every 1 sec
* containing their id, their current speed (kmh), overall elapsed distance (m)
* and a timestamp. The streaming example triggers the top speed of each car
* every x meters elapsed for the last y seconds.
*/
public class TopSpeedWindowingExample {

	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		@SuppressWarnings({"rawtypes", "serial"})
		DataStream<Tuple4<Integer, Integer, Double, Long>> carData;
		if (fileInput) {
			carData = env.readTextFile(inputPath).map(new ParseCarData());
		} else {
			carData = env.addSource(CarSource.create(numOfCars));
		}
		DataStream<Tuple4<Integer, Integer, Double, Long>> topSpeeds = carData.groupBy(0)
				.window(Time.of(evictionSec, new CarTimestamp()))
				.every(Delta.of(triggerMeters,
						new DeltaFunction<Tuple4<Integer, Integer, Double, Long>>() {
							@Override
							public double getDelta(
									Tuple4<Integer, Integer, Double, Long> oldDataPoint,
									Tuple4<Integer, Integer, Double, Long> newDataPoint) {
								return newDataPoint.f2 - oldDataPoint.f2;
							}
						}, new Tuple4<Integer, Integer, Double, Long>(0, 0, 0d, 0l))).local().maxBy(1).flatten();
		if (fileOutput) {
			topSpeeds.writeAsText(outputPath);
		} else {
			topSpeeds.print();
		}

		env.execute("CarTopSpeedWindowingExample");
	}

	private static class CarSource implements SourceFunction<Tuple4<Integer, Integer, Double, Long>> {

		private static final long serialVersionUID = 1L;
		private Integer[] speeds;
		private Double[] distances;

		private Random rand = new Random();

		private int carId = 0;

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
		public boolean reachedEnd() throws Exception {
			return false;
		}

		@Override
		public Tuple4<Integer, Integer, Double, Long> next() throws Exception {
			if (rand.nextBoolean()) {
				speeds[carId] = Math.min(100, speeds[carId] + 5);
			} else {
				speeds[carId] = Math.max(0, speeds[carId] - 5);
			}
			distances[carId] += speeds[carId] / 3.6d;
			Tuple4<Integer, Integer, Double, Long> record = new Tuple4<Integer, Integer, Double, Long>(carId,
					speeds[carId], distances[carId], System.currentTimeMillis());
			carId++;
			if (carId >= speeds.length) {
				carId = 0;
			}
			return record;
		}

	}

	private static class ParseCarData extends
			RichMapFunction<String, Tuple4<Integer, Integer, Double, Long>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple4<Integer, Integer, Double, Long> map(String record) {
			String rawData = record.substring(1, record.length() - 1);
			String[] data = rawData.split(",");
			return new Tuple4<Integer, Integer, Double, Long>(Integer.valueOf(data[0]),
					Integer.valueOf(data[1]), Double.valueOf(data[2]), Long.valueOf(data[3]));
		}
	}

	private static class CarTimestamp implements Timestamp<Tuple4<Integer, Integer, Double, Long>> {

		@Override
		public long getTimestamp(Tuple4<Integer, Integer, Double, Long> value) {
			return value.f3;
		}
	}

	private static boolean fileInput = false;
	private static boolean fileOutput = false;
	private static int numOfCars = 2;
	private static int evictionSec = 10;
	private static double triggerMeters = 50;
	private static String inputPath;
	private static String outputPath;

	private static boolean parseParameters(String[] args) {

		if (args.length > 0) {
			if (args.length == 3) {
				numOfCars = Integer.valueOf(args[0]);
				evictionSec = Integer.valueOf(args[1]);
				triggerMeters = Double.valueOf(args[2]);
			} else if (args.length == 2) {
				fileInput = true;
				fileOutput = true;
				inputPath = args[0];
				outputPath = args[1];
			} else {
				System.err
						.println("Usage: TopSpeedWindowingExample <numCars> <evictSec> <triggerMeters>");
				return false;
			}
		}
		return true;
	}
}
