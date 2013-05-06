/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.example.kmeans;

import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Calendar;
import java.util.Locale;
import java.util.Random;

public class KMeansSampleDataGenerator {

	/**
	 * Generates data sets for the KMeansIteration example PACT program. Data
	 * points and cluster centers have an id and two coordinates (x and y).
	 * Coordinates are double values ranging from 0 to 1. Each line of an output
	 * file represents a point or a center and has the following format:
	 * "id|x-coordinate|y-coordinate|".
	 * One or multiple files holding the data points are generated. Those are
	 * named "dataPoints_$" where $ is the id of the file. One file holding
	 * centers is generated and named "centers_0". All files are place in the
	 * output directory.
	 * 
	 * @param args
	 *        Parameters for the data set generation are: 1) the number of
	 *        data points to be generated, 2) the number of files into which
	 *        the data points will be splitted up, 3) the number of cluster
	 *        centers to be generated, 4) the output directory.
	 */
	public static void main(String[] args) {

		// check parameter count
		if (args.length != 4) {
			System.out
				.println("KMeansDataGenerator <numberOfDataPoints> <numberOfDataPointFiles> <numberOfClusterCenters> <outputDir>");
			System.exit(-1);
		}

		// parse parameters
		int noDataPoints = Integer.parseInt(args[0]);
		int noDataPointFiles = Integer.parseInt(args[1]);
		int noClusterCenters = Integer.parseInt(args[2]);
		String outputDir = args[3];

		// generate data point files
		genPointFiles(noDataPoints, 2, (noClusterCenters * 3), noDataPointFiles, outputDir + "/dataPoints_");
		// generate centers files
		genClusterFiles(noClusterCenters, 2, 1, outputDir + "/centers_");

	}

	/**
	 * Generates files with data points. Prior to data point generation, a
	 * number of centers are generated around which the data points are
	 * positioned. The coordinates of the points are double values that range
	 * from 0 to 1.
	 * 
	 * @param noPoints
	 *        Total number of points to generate.
	 * @param noDims
	 *        Number of coordinates a point consists of (dimension of
	 *        point).
	 * @param noCenters
	 *        Number of centers around which the points are randomly
	 *        positioned.
	 * @param noFiles
	 *        Number of files into which the points are written.
	 * @param path
	 *        Prefix of the path of the generated files.
	 */
	private static void genPointFiles(int noPoints, int noDims, int noCenters, int noFiles, String path) {

		// Enforce '.' as decimal character
		Locale.setDefault(Locale.US);
		DecimalFormat df = new DecimalFormat("#0.00");

		// initialize random number generator
		Random rand = new Random(Calendar.getInstance().getTimeInMillis());

		// initialize centers around which points will be placed
		double[][] centers = new double[noCenters][noDims];
		for (int i = 0; i < noCenters; i++) {
			for (int j = 0; j < noDims; j++) {
				centers[i][j] = rand.nextDouble();
			}
		}

		// init counter
		int fileId = 0;
		int pointInFile = 0;

		// compute number of points per file
		int pointsPerFile = (noPoints / noFiles) + 1;

		// init point
		double[] point = new double[noDims];

		try {
			// create file writer for first data point file
			FileWriter fw = new FileWriter(path + (fileId++));

			// for all points to be generated
			for (int i = 0; i < noPoints; i++) {

				// determine center around which point is positioned
				int centerId = rand.nextInt(noCenters);
				// determine each coordinate of the point
				for (int j = 0; j < noDims; j++) {
					point[j] = centers[centerId][j] + rand.nextGaussian();
					while (point[j] < 0 || point[j] > 1) {
						point[j] = centers[centerId][j] + rand.nextGaussian();
					}
				}

				// write the point to the file
				fw.write(pointToString(i, point, df) + "\n");

				// check whether to open a new file
				pointInFile++;
				if (pointInFile == pointsPerFile) {
					// close current file
					fw.close();
					// open new file writer
					fw = new FileWriter(path + (fileId++));
					// reset pointInFile counter
					pointInFile = 0;
				}
			}
			// close last file
			fw.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Generates files with random cluster center points. The coordinates of the
	 * centers are double values that range from 0 to 1.
	 * 
	 * @param noCenters
	 *        Total number of centers to be generated.
	 * @param noDims
	 *        Number of coordinates a point consists of (dimension of
	 *        point).
	 * @param noFiles
	 *        Number of files into which the centers are written.
	 * @param path
	 *        Prefix of the path of the generated files.
	 */
	private static void genClusterFiles(int noCenters, int noDims, int noFiles, String path) {

		// Enforce '.' as decimal character
		Locale.setDefault(Locale.US);
		DecimalFormat df = new DecimalFormat("#0.00");

		// Initialize random number generator
		Random rand = new Random(Calendar.getInstance().getTimeInMillis());

		// init counter
		int fileId = 0;
		int pointInFile = 0;

		// compute number of centers per file
		int pointsPerFile = (noCenters / noFiles) + 1;

		// init center
		double[] center = new double[noDims];

		try {
			// create file writer for the first center file
			FileWriter fw = new FileWriter(path + (fileId++));

			// for each cluster center
			for (int i = 0; i < noCenters; i++) {

				// generate all coordinates
				for (int j = 0; j < noDims; j++) {
					center[j] = rand.nextDouble();
				}

				// write center to file
				fw.write(pointToString(i, center, df) + "\n");

				// check if new file must be opened
				pointInFile++;
				if (pointInFile == pointsPerFile) {
					// close current cluster file
					fw.close();
					// create a new file writer for the next cluster file
					fw = new FileWriter(path + (fileId++));
					// reset pointInFile counter
					pointInFile = 0;
				}
			}
			// close last cluster file
			fw.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Serializes a data point or cluster center into a string. Id and
	 * coordinates are separated by the '|' character. The string ends with '|'
	 * as well.
	 * 
	 * @param pointId
	 *        Id of the data point or cluster center.
	 * @param coords
	 *        Coordinates of the data point or cluster center.
	 * @param df
	 *        DecimalFormat to format the double values of the coordinates.
	 * @return A string representing the data point or cluster center
	 */
	private static String pointToString(int pointId, double[] coords, DecimalFormat df) {
		// Enforce '.' as decimal separator
		Locale.setDefault(Locale.US);

		// init string builder
		StringBuilder sb = new StringBuilder();

		// append id
		sb.append(pointId);
		sb.append('|');

		// append all coordinates
		for (int j = 0; j < coords.length; j++) {
			sb.append(df.format(coords[j]));
			sb.append('|');
		}
		// return string
		return sb.toString();
	}
}
