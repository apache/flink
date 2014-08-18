/**
 *
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
 *
 */

package org.apache.flink.streaming.examples.iterative.kmeans;

import java.util.Random;

import org.apache.flink.streaming.api.function.source.SourceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class KMeansSource implements SourceFunction<Tuple2<String, Long>> {
	private static final long serialVersionUID = 1L;
	private final long DEFAULT_SEED = 4650285087650871364L;
	private Random random = new Random(DEFAULT_SEED);
	private Tuple2<String, Long> outRecord = new Tuple2<String, Long>();
	private int numCenter;
	private int dimension;
	private double absoluteStdDev;
	private double range;
	private StringBuilder buffer = new StringBuilder();
	
	public KMeansSource(int numCenter, int dimension, double stddev, double range){
		this.numCenter=numCenter;
		this.dimension=dimension;
		this.absoluteStdDev = stddev * range;
		this.range=range;
	}
	
	@Override
	public void invoke(Collector<Tuple2<String, Long>> collector)
			throws Exception {
		double[][] means = uniformRandomCenters(random, numCenter, dimension, range);
		double[] point = new double[dimension];
		int nextCentroid = 0;
		
		while (true) {
			// generate a point for the current centroid
			double[] centroid = means[nextCentroid];
			for (int d = 0; d < dimension; d++) {
				point[d] = (random.nextGaussian() * absoluteStdDev) + centroid[d];
			}
			nextCentroid = (nextCentroid + 1) % numCenter;
			String pointString=generatePointString(point);
			outRecord.f0 = pointString;
			collector.collect(outRecord);
		}
	}
	
	private double[][] uniformRandomCenters(Random rnd, int num, int dimensionality, double range) {
		final double halfRange = range / 2;
		final double[][] points = new double[num][dimensionality];
		for (int i = 0; i < num; i++) {
			for (int dim = 0; dim < dimensionality; dim ++) {
				points[i][dim] = (rnd.nextDouble() * range) - halfRange;
			}
		}
		return points;
	}
	
	private String generatePointString(double[] point){
		buffer.setLength(0);
		for (int j = 0; j < dimension; j++) {
			buffer.append(point[j]);
			if(j < dimension - 1) {
				buffer.append(" ");
			}
		}
		return buffer.toString();
	}

}
