/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.streaming.examples.iterative.kmeans;

import java.util.Random;

import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.streaming.api.invokable.UserSourceInvokable;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;

public class KMeansSource extends UserSourceInvokable {
	private static final long serialVersionUID = 1L;
	private static final long DEFAULT_SEED = 4650285087650871364L;
	private Random random = new Random(DEFAULT_SEED);
	private StreamRecord outRecord = new StreamRecord(new Tuple1<String>());
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
	public void invoke() throws Exception {
		// TODO Auto-generated method stub
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
			outRecord.setString(0, pointString);
			emit(outRecord);
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
