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

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.example.java.clustering.KMeans.Centroid;
import eu.stratosphere.example.java.clustering.KMeans.Point;

public class KMeansData {

	public static DataSet<Centroid> getDefaultCentroidDataSet(ExecutionEnvironment env) {
		
		return env.fromElements(
				new Centroid(1, -31.85, -44.77),
				new Centroid(2, 35.16, 17.46),
				new Centroid(3, -5.16, 21.93),
				new Centroid(4, -24.06, 6.81)
				);
	}
	
	public static DataSet<Point> getDefaultPointDataSet(ExecutionEnvironment env) {
		
		return env.fromElements(
				new Point(-14.22, -48.01),
				new Point(-22.78, 37.10),
				new Point(56.18, -42.99),
				new Point(35.04, 50.29),
				new Point(-9.53, -46.26),
				new Point(-34.35, 48.25),
				new Point(55.82, -57.49),
				new Point(21.03, 54.64),
				new Point(-13.63, -42.26),
				new Point(-36.57, 32.63),
				new Point(50.65, -52.40),
				new Point(24.48, 34.04),
				new Point(-2.69, -36.02),
				new Point(-38.80, 36.58),
				new Point(24.00, -53.74),
				new Point(32.41, 24.96),
				new Point(-4.32, -56.92),
				new Point(-22.68, 29.42),
				new Point(59.02, -39.56),
				new Point(24.47, 45.07),
				new Point(5.23, -41.20),
				new Point(-23.00, 38.15),
				new Point(44.55, -51.50),
				new Point(14.62, 59.06),
				new Point(7.41, -56.05),
				new Point(-26.63, 28.97),
				new Point(47.37, -44.72),
				new Point(29.07, 51.06),
				new Point(0.59, -31.89),
				new Point(-39.09, 20.78),
				new Point(42.97, -48.98),
				new Point(34.36, 49.08),
				new Point(-21.91, -49.01),
				new Point(-46.68, 46.04),
				new Point(48.52, -43.67),
				new Point(30.05, 49.25),
				new Point(4.03, -43.56),
				new Point(-37.85, 41.72),
				new Point(38.24, -48.32),
				new Point(20.83, 57.85)
				);
	}
	
}
