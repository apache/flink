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

package eu.stratosphere.example.java.clustering.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Locale;
import java.util.Random;

import eu.stratosphere.example.java.clustering.KMeans;

/**
 * Generates data for the {@link KMeans} example program.
 */
public class KMeansDataGenerator {
	
	static {
		Locale.setDefault(Locale.US);
	}
	
	private static final String CENTERS_FILE = "centers";
	private static final String POINTS_FILE = "points";
	private static final long DEFAULT_SEED = 4650285087650871364L;
	private static final double DEFAULT_VALUE_RANGE = 100.0;
	private static final double RELATIVE_STDDEV = 0.08;
	private static final int DIMENSIONALITY = 2;
	private static final DecimalFormat FORMAT = new DecimalFormat("#0.00");
	private static final char DELIMITER = ' ';

	/**
	 * Main method to generate data for the {@link KMeans} example program.
	 * <p>
	 * The generator creates to files:
	 * <ul>
	 * <li><code>{tmp.dir}/points</code> for the data points
	 * <li><code>{tmp.dir}/centers</code> for the cluster centers
	 * </ul> 
	 * 
	 * @param args 
	 * <ol>
	 * <li>Int: Number of data points
	 * <li>Int: Number of cluster centers
	 * <li><b>Optional</b> Double: Standard deviation of data points
	 * <li><b>Optional</b> Double: Value range of cluster centers
	 * <li><b>Optional</b> Long: Random seed
	 * </ol>
	 */
	public static void main(String[] args) throws IOException {

		// check parameter count
		if (args.length < 2) {
			System.out.println("KMeansDataGenerator <numberOfDataPoints> <numberOfClusterCenters> [<relative stddev>] [<centroid range>] [<seed>]");
			System.exit(1);
		}

		// parse parameters
		final int numDataPoints = Integer.parseInt(args[0]);
		final int k = Integer.parseInt(args[1]);
		final double stddev = args.length > 2 ? Double.parseDouble(args[2]) : RELATIVE_STDDEV;
		final double range = args.length > 3 ? Double.parseDouble(args[4]) : DEFAULT_VALUE_RANGE;
		final long firstSeed = args.length > 4 ? Long.parseLong(args[4]) : DEFAULT_SEED;
		
		final double absoluteStdDev = stddev * range;
		final Random random = new Random(firstSeed);
		final String tmpDir = System.getProperty("java.io.tmpdir");
		
		// the means around which data points are distributed
		final double[][] means = uniformRandomCenters(random, k, DIMENSIONALITY, range);
		
		// write the points out
		BufferedWriter pointsOut = null;
		try {
			pointsOut = new BufferedWriter(new FileWriter(new File(tmpDir+"/"+POINTS_FILE)));
			StringBuilder buffer = new StringBuilder();
			
			double[] point = new double[DIMENSIONALITY];
			int nextCentroid = 0;
			
			for (int i = 1; i <= numDataPoints; i++) {
				// generate a point for the current centroid
				double[] centroid = means[nextCentroid];
				for (int d = 0; d < DIMENSIONALITY; d++) {
					point[d] = (random.nextGaussian() * absoluteStdDev) + centroid[d];
				}
				writePoint(point, buffer, pointsOut);
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
			centersOut = new BufferedWriter(new FileWriter(new File(tmpDir+"/"+CENTERS_FILE)));
			StringBuilder buffer = new StringBuilder();
			
			double[][] centers = uniformRandomCenters(random, k, DIMENSIONALITY, range);
			
			for (int i = 0; i < k; i++) {
				writeCenter(i + 1, centers[i], buffer, centersOut);
			}
		}
		finally {
			if (centersOut != null) {
				centersOut.close();
			}
		}
		
		System.out.println("Wrote "+numDataPoints+" data points to "+tmpDir+"/"+POINTS_FILE);
		System.out.println("Wrote "+k+" cluster centers to "+tmpDir+"/"+CENTERS_FILE);
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
	
	private static void writePoint(double[] coordinates, StringBuilder buffer, BufferedWriter out) throws IOException {
		buffer.setLength(0);
		
		// write coordinates
		for (int j = 0; j < coordinates.length; j++) {
			buffer.append(FORMAT.format(coordinates[j]));
			if(j < coordinates.length - 1) {
				buffer.append(DELIMITER);
			}
		}
		
		out.write(buffer.toString());
		out.newLine();
	}
	
	private static void writeCenter(long id, double[] coordinates, StringBuilder buffer, BufferedWriter out) throws IOException {
		buffer.setLength(0);
		
		// write id
		buffer.append(id);
		buffer.append(DELIMITER);

		// write coordinates
		for (int j = 0; j < coordinates.length; j++) {
			buffer.append(FORMAT.format(coordinates[j]));
			if(j < coordinates.length - 1) {
				buffer.append(DELIMITER);
			}
		}
		
		out.write(buffer.toString());
		out.newLine();
	}
}
