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

package org.apache.flink.test.iterative;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.examples.java.clustering.KMeans;
import org.apache.flink.examples.java.clustering.KMeans.Centroid;
import org.apache.flink.examples.java.clustering.KMeans.Point;
import org.apache.flink.test.testdata.KMeansData;
import org.apache.flink.test.util.JavaProgramTestBase;

import java.util.List;
import java.util.Locale;

/**
 * Test KMeans clustering with a broadcast set.
 */
public class KMeansWithBroadcastSetITCase extends JavaProgramTestBase {

	@SuppressWarnings("serial")
	@Override
	protected void testProgram() throws Exception {

		String[] points = KMeansData.DATAPOINTS_2D.split("\n");
		String[] centers = KMeansData.INITIAL_CENTERS_2D.split("\n");

		// set up execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// get input data
		DataSet<Point> pointsSet = env.fromElements(points)
				.map(new MapFunction<String, Point>() {
					public Point map(String p) {
						String[] fields = p.split("\\|");
						return new Point(
								Double.parseDouble(fields[1]),
								Double.parseDouble(fields[2]));
					}
				});

		DataSet <Centroid> centroidsSet = env.fromElements(centers)
				.map(new MapFunction<String, Centroid>() {
					public Centroid map(String c) {
						String[] fields = c.split("\\|");
						return new Centroid(
								Integer.parseInt(fields[0]),
								Double.parseDouble(fields[1]),
								Double.parseDouble(fields[2]));
					}
				});

		// set number of bulk iterations for KMeans algorithm
		IterativeDataSet<Centroid> loop = centroidsSet.iterate(20);

		DataSet<Centroid> newCentroids = pointsSet
				// compute closest centroid for each point
				.map(new KMeans.SelectNearestCenter()).withBroadcastSet(loop, "centroids")
						// count and sum point coordinates for each centroid
				.map(new KMeans.CountAppender())
				.groupBy(0).reduce(new KMeans.CentroidAccumulator())
						// compute new centroids from point counts and coordinate sums
				.map(new KMeans.CentroidAverager());

		// feed new centroids back into next iteration
		DataSet<Centroid> finalCentroids = loop.closeWith(newCentroids);

		DataSet<String> stringCentroids = finalCentroids
				.map(new MapFunction<Centroid, String>() {
					@Override
					public String map(Centroid c) throws Exception {
						return String.format(Locale.US, "%d|%.2f|%.2f|", c.id, c.x, c.y);
					}
				});

		List<String> result = stringCentroids.collect();

		KMeansData.checkResultsWithDelta(
				KMeansData.CENTERS_2D_AFTER_20_ITERATIONS_DOUBLE_DIGIT,
				result,
				0.01);
	}

}
