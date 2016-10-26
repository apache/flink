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

package org.apache.flink.examples.java.ap.util;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.Collector;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * Provides the default data set used for the Single Source Shortest Paths example program.
 * If no parameters are given to the program, the default edge data set is used.
 */
public class AffinityPropagationData {

	//public static final Integer MAX_ITERATIONS = 4;

	public static DataSet<Tuple3<LongValue, LongValue, DoubleValue>> getTuples(ExecutionEnvironment env) {

		List<Tuple3<LongValue, LongValue, DoubleValue>> edges = new ArrayList<>();
		edges.add(new Tuple3<>(new LongValue(1L), new LongValue(1L), new DoubleValue(1.0)));
		edges.add(new Tuple3<>(new LongValue(1L), new LongValue(2L), new DoubleValue(1.0)));
		edges.add(new Tuple3<>(new LongValue(1L), new LongValue(3L), new DoubleValue(5.0)));
		edges.add(new Tuple3<>(new LongValue(1L), new LongValue(4L), new DoubleValue(1.0)));
		edges.add(new Tuple3<>(new LongValue(2L), new LongValue(1L), new DoubleValue(1.0)));
		edges.add(new Tuple3<>(new LongValue(2L), new LongValue(2L), new DoubleValue(1.0)));
		edges.add(new Tuple3<>(new LongValue(2L), new LongValue(3L), new DoubleValue(2.0)));
		edges.add(new Tuple3<>(new LongValue(2L), new LongValue(4L), new DoubleValue(6.0)));
		edges.add(new Tuple3<>(new LongValue(3L), new LongValue(1L), new DoubleValue(5.0)));
		edges.add(new Tuple3<>(new LongValue(3L), new LongValue(2L), new DoubleValue(2.0)));
		edges.add(new Tuple3<>(new LongValue(3L), new LongValue(3L), new DoubleValue(1.0)));
		edges.add(new Tuple3<>(new LongValue(3L), new LongValue(4L), new DoubleValue(2.0)));
		edges.add(new Tuple3<>(new LongValue(4L), new LongValue(1L), new DoubleValue(1.0)));
		edges.add(new Tuple3<>(new LongValue(4L), new LongValue(2L), new DoubleValue(6.0)));
		edges.add(new Tuple3<>(new LongValue(4L), new LongValue(3L), new DoubleValue(2.0)));
		edges.add(new Tuple3<>(new LongValue(4L), new LongValue(4L), new DoubleValue(1.0)));

		return env.fromCollection(edges);
	}

	public static DataSet<Tuple3<LongValue, LongValue, DoubleValue>> getTuplesFromFile(ExecutionEnvironment env) {

		DataSet<String> text =
			env.readTextFile("file:////Users/joseprubio/Documents/dev/APpython/stockdailypropcovariance.txt");

		DataSet<Tuple3<LongValue, LongValue, DoubleValue>> similarities =
			text.map(new MapFunction<String, List<Tuple3<LongValue, LongValue, DoubleValue>>>() {
			@Override
			public List<Tuple3<LongValue, LongValue, DoubleValue>> map(String value) {

				List<Tuple3<LongValue, LongValue, DoubleValue>> similarities = new ArrayList<>();

				String[] values = value.split(" ");

				for(int i = 1; i < values.length; i++){
					similarities.add(new Tuple3<>(new LongValue(Long.parseLong(values[0])),
						new LongValue(i), new DoubleValue(Double.parseDouble(values[i]))));
				}

				return similarities;
			}
		}).reduceGroup(new GroupReduceFunction<List<Tuple3<LongValue, LongValue, DoubleValue>>,
				Tuple3<LongValue, LongValue, DoubleValue>>() {


				@Override
				public void reduce(Iterable<List<Tuple3<LongValue, LongValue, DoubleValue>>> maxValues,
					Collector<Tuple3<LongValue, LongValue, DoubleValue>> out)
					throws Exception {

					for (List<Tuple3<LongValue, LongValue, DoubleValue>> value : maxValues) {
						for(Tuple3<LongValue, LongValue, DoubleValue> value2: value){
							out.collect(value2);
						}
					}
				}
			});

		return similarities;
	}

	public static double[][] getStockDataset() throws IOException {

		Scanner inFile = new Scanner(new File("input.txt")).useDelimiter(" ");

		List<Float> temps = new ArrayList<>();

		ArrayList<ArrayList<Double>> arrayLists = new ArrayList<>();

		while (inFile.hasNextLine())
		{
			ArrayList<Double> singleList = new ArrayList<>();
			Scanner line = new Scanner(inFile.nextLine()).useDelimiter(" ");
			while (line.hasNext()) {
				double token1 = line.nextDouble();
				singleList.add(token1);
			}
			arrayLists.add(singleList);
			line.close();

		}

		inFile.close();

		double[][] data = new double[arrayLists.size()][arrayLists.size()];

		int i = 0;
		int j = 0;

		for (ArrayList<Double> it : arrayLists) {

			for(Double sim : it){
				data[i][j] = sim;
				j++;
			}
			j = 0;
			i++;
		}

		return data;
	}

	private AffinityPropagationData() {}
}
