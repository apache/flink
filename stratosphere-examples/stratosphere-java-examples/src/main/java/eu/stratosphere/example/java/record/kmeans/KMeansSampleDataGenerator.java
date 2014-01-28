/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.example.java.record.kmeans;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Locale;
import java.util.Random;

public class KMeansSampleDataGenerator {
	
	static {
		Locale.setDefault(Locale.US);
	}
	
	private static final String CENTERS_FILE = "centers";
	
	private static final String POINTS_FILE = "points";
	
	private static final long DEFAULT_SEED = 4650285087650871364L;
	
	private static final double DEFAULT_VALUE_RANGE = 100.0;
	
	private static final double RELATIVE_STDDEV = 0.15;
	
	private static final int DIMENSIONALITY = 2;
	
	
	private static final DecimalFormat FORMAT = new DecimalFormat("#0.00");
	
	private static final char DELIMITER = '|';
	


	public static void main(String[] args) throws IOException {

		// check parameter count
		if (args.length < 2) {
			System.out.println("KMeansDataGenerator <numberOfDataPoints> <numberOfClusterCenters> [<centroid range>] [<seed>]");
			System.exit(1);
		}

		// parse parameters
		final int numDataPoints = Integer.parseInt(args[0]);
		final int k = Integer.parseInt(args[1]);
		
		final double range = args.length > 2 ? Double.parseDouble(args[2]) : DEFAULT_VALUE_RANGE;
		final long firstSeed = args.length > 3 ? Long.parseLong(args[3]) : DEFAULT_SEED;
		
		// generate the centers first
		final double absoluteStdDev = RELATIVE_STDDEV * range;
		final Random random = new Random(firstSeed);
		
		
		// the means for our gaussian distributions
		final double[][] means = uniformRandomCenters(random, k, DIMENSIONALITY, range);
		
		// write the points out
		BufferedWriter pointsOut = null;
		try {
			pointsOut = new BufferedWriter(new FileWriter(new File(POINTS_FILE)));
			StringBuilder buffer = new StringBuilder();
			
			double[] point = new double[DIMENSIONALITY];
			int nextCentroid = 0;
			
			for (int i = 1; i <= numDataPoints; i++) {
				// generate a point for the current centroid
				double[] centroid = means[nextCentroid];
				for (int d = 0; d < DIMENSIONALITY; d++) {
					point[d] = (random.nextGaussian() * absoluteStdDev) + centroid[d];
				}
				write(i, point, buffer, pointsOut);
				nextCentroid = (nextCentroid + 1) % k;
			}
		}
		finally {
			if (pointsOut != null) {
				pointsOut.close();
			}
		}

		
		// write the uniformly distributed centers to a file
		BufferedWriter centersOut = null;
		try {
			centersOut = new BufferedWriter(new FileWriter(new File(CENTERS_FILE)));
			StringBuilder buffer = new StringBuilder();
			
			double[][] centers = uniformRandomCenters(random, k, DIMENSIONALITY, range);
			
			for (int i = 0; i < k; i++) {
				write(i + 1, centers[i], buffer, centersOut);
			}
		}
		finally {
			if (centersOut != null) {
				centersOut.close();
			}
		}
	}
	
	private static final double[][] uniformRandomCenters(Random rnd, int num, int dimensionality, double range) {
		final double halfRange = range / 2;
		final double[][] points = new double[num][dimensionality];
		
		for (int i = 0; i < num; i++) {
			for (int dim = 0; dim < dimensionality; dim ++) {
				points[i][dim] = (rnd.nextDouble() * range) - halfRange;
			}
		}
		return points;
	}
	
	private static void write(long id, double[] coordinates, StringBuilder buffer, BufferedWriter out) throws IOException {
		buffer.setLength(0);
		
		buffer.append(id);
		buffer.append(DELIMITER);

		// append all coordinates
		for (int j = 0; j < coordinates.length; j++) {
			buffer.append(FORMAT.format(coordinates[j]));
			buffer.append('|');
		}
		
		out.write(buffer.toString());
		out.newLine();
	}
}
