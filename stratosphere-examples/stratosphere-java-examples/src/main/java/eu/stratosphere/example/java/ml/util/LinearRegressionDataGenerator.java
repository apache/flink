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

package eu.stratosphere.example.java.ml.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Locale;
import java.util.Random;

/**
 * Generates data for the {@link LinearRegression} example program.
 */
public class LinearRegressionDataGenerator {

	static {
		Locale.setDefault(Locale.US);
	}

	private static final String POINTS_FILE = "data";
	private static final long DEFAULT_SEED = 4650285087650871364L;
	private static final int DIMENSIONALITY = 1;
	private static final DecimalFormat FORMAT = new DecimalFormat("#0.00");
	private static final char DELIMITER = ' ';

	/**
	 * Main method to generate data for the {@link LinearRegression} example program.
	 * <p>
	 * The generator creates to files:
	 * <ul>
	 * <li><code>{tmp.dir}/data</code> for the data points
	 * </ul> 
	 * 
	 * @param args 
	 * <ol>
	 * <li>Int: Number of data points
	 * <li><b>Optional</b> Long: Random seed
	 * </ol>
	 */
	public static void main(String[] args) throws IOException {

		System.out.println(args.length);

		// check parameter count
		if (args.length < 1) {
			System.out.println("LinearRegressionDataGenerator <numberOfDataPoints> [<seed>]");
			System.exit(1);
		}

		// parse parameters
		final int numDataPoints = Integer.parseInt(args[0]);
		final long firstSeed = args.length > 1 ? Long.parseLong(args[4]) : DEFAULT_SEED;
		final Random random = new Random(firstSeed);
		final String tmpDir = System.getProperty("java.io.tmpdir");

		// write the points out
		BufferedWriter pointsOut = null;
		try {
			pointsOut = new BufferedWriter(new FileWriter(new File(POINTS_FILE)));
			StringBuilder buffer = new StringBuilder();

			// DIMENSIONALITY + 1 means that the number of x(dimensionality) and target y
			double[] point = new double[DIMENSIONALITY+1];

			for (int i = 1; i <= numDataPoints; i++) {
				point[0] = random.nextGaussian();
				point[1] = 2 * point[0] + 0.01*random.nextGaussian();
				writePoint(point, buffer, pointsOut);
			}

		}
		finally {
			if (pointsOut != null) {
				pointsOut.close();
			}
		}

		System.out.println("Wrote "+numDataPoints+" data points to "+tmpDir+"/"+POINTS_FILE);
	}


	private static void writePoint(double[] data, StringBuilder buffer, BufferedWriter out) throws IOException {
		buffer.setLength(0);

		// write coordinates
		for (int j = 0; j < data.length; j++) {
			buffer.append(FORMAT.format(data[j]));
			if(j < data.length - 1) {
				buffer.append(DELIMITER);
			}
		}

		out.write(buffer.toString());
		out.newLine();
	}
}
