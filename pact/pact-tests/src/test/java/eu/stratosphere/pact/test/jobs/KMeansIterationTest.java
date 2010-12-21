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

package eu.stratosphere.pact.test.jobs;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.Collection;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.StringTokenizer;

import junit.framework.Assert;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.jobgen.JobGraphGenerator;
import eu.stratosphere.pact.compiler.plan.OptimizedPlan;
import eu.stratosphere.pact.example.datamining.KMeansIteration;
import eu.stratosphere.pact.test.util.TestBase;

@RunWith(Parameterized.class)
public class KMeansIterationTest extends TestBase {

	KMeanDataGenerator kmdg = new KMeanDataGenerator(500, 10, 2);

	String dataPath = getFilesystemProvider().getTempDirPath() + "/dataPoints";

	String clusterPath = getFilesystemProvider().getTempDirPath() + "/clusters";

	String resultPath = clusterPath + "/iter_1.txt";

	public KMeansIterationTest(Configuration config) {
		super(config);
	}

	@Override
	protected String getJarFilePath() {
		return "/home/fhueske/distTests.jar";
	}

	@Override
	protected void preSubmit() throws Exception {

		String dataFile = kmdg.getDataPoints();
		String clusterFile = kmdg.getClusterCenters();

		int noPartitions = 4;
		int partitionSize = (dataFile.length() / noPartitions) - 2;

		// create data path
		getFilesystemProvider().createDir(dataPath);

		// TODO: check splitting!
		// split data file and copy parts
		for (int i = 0; i < noPartitions; i++) {
			int cutPos = dataFile.indexOf('\n', (partitionSize < dataFile.length() ? partitionSize
				: (dataFile.length() - 1)));
			getFilesystemProvider().createFile(dataPath + "/part_" + i + ".txt", dataFile.substring(0, cutPos) + "\n");
			System.out.println("Points Part " + (i + 1) + ":\n>" + dataFile.substring(0, cutPos) + "\n<");
			dataFile = dataFile.substring(cutPos + 1);
		}

		// create cluster path and copy data
		getFilesystemProvider().createDir(clusterPath);
		getFilesystemProvider().createFile(clusterPath + "/iter_0.txt", clusterFile);
		System.out.println("Clusters: \n>" + clusterFile + "<");

	}

	@Override
	protected JobGraph getJobGraph() throws Exception {

		KMeansIteration kmi = new KMeansIteration();
		// Plan plan = kmi.getPlan(dataPath,clusterPath, clusterPath+"/iter_1.txt",
		// config.getString("KMeansIterationTest#NoSubtasks", "1"));
		Plan plan = kmi.getPlan(dataPath, clusterPath, resultPath, config.getString(
			"KMeansIterationTest#NoSubtasks", "1"));

		PactCompiler pc = new PactCompiler();
		OptimizedPlan op = pc.compile(plan);

		JobGraphGenerator jgg = new JobGraphGenerator();
		return jgg.compileJobGraph(op);

	}

	@Override
	protected void postSubmit() throws Exception {

		// Test results

		// read result
		InputStream is = getFilesystemProvider().getInputStream(clusterPath + "/iter_1.txt");
		BufferedReader reader = new BufferedReader(new InputStreamReader(is));
		String line = reader.readLine();
		Assert.assertNotNull("No output computed", line);

		// collect out lines
		PriorityQueue<String> computedResult = new PriorityQueue<String>();
		while (line != null) {
			computedResult.add(line);
			line = reader.readLine();
		}
		reader.close();

		PriorityQueue<String> expectedResult = new PriorityQueue<String>();
		StringTokenizer st = new StringTokenizer(kmdg.getNewClusterCenters(), "\n");
		while (st.hasMoreElements()) {
			expectedResult.add(st.nextToken());
		}

		// print expected and computed results
		System.out.println("Expected: " + expectedResult);
		System.out.println("Computed: " + computedResult);

		Assert.assertEquals("Computed and expected results have different size", expectedResult.size(), computedResult
			.size());

		while (!expectedResult.isEmpty()) {
			String expectedLine = expectedResult.poll();
			String computedLine = computedResult.poll();
			System.out.println("expLine: <" + expectedLine + ">\t\t: compLine: <" + computedLine + ">");

			Assert.assertEquals("Computed and expected lines differ", expectedLine, computedLine);
		}

		// clean up hdfs
		getFilesystemProvider().delete(dataPath, true);
		getFilesystemProvider().delete(clusterPath, true);

	}

	@Parameters
	public static Collection<Object[]> getConfigurations() {

		LinkedList<Configuration> tConfigs = new LinkedList<Configuration>();

		Configuration config = new Configuration();
		config.setInteger("KMeansIterationTest#NoSubtasks", 2);
		tConfigs.add(config);

		return toParameterList(tConfigs);
	}

	public static class KMeanDataGenerator {

		int noPoints;

		int noClusters;

		int noDims;

		Random rand = new Random(System.currentTimeMillis());

		double[][] dataPoints;

		double[][] centers;

		double[][] newCenters;

		public KMeanDataGenerator(int noPoints, int noClusters, int noDims) {
			this.noPoints = noPoints;
			this.noClusters = noClusters;
			this.noDims = noDims;

			this.dataPoints = new double[noPoints][noDims];
			this.centers = new double[noClusters][noDims];
			this.newCenters = new double[noClusters][noDims];

			// init data points
			for (int i = 0; i < noPoints; i++) {
				for (int j = 0; j < noDims; j++) {
					dataPoints[i][j] = rand.nextDouble() * 100;
				}
			}

			// init centers
			for (int i = 0; i < noClusters; i++) {
				for (int j = 0; j < noDims; j++) {
					centers[i][j] = rand.nextDouble() * 100;
				}
			}

			// compute new centers
			int[] dataPointCnt = new int[noClusters];

			for (int i = 0; i < noPoints; i++) {

				double minDist = Double.MAX_VALUE;
				int nearestCluster = 0;
				for (int j = 0; j < noClusters; j++) {
					double dist = computeDistance(dataPoints[i], centers[j]);
					if (dist < minDist) {
						minDist = dist;
						nearestCluster = j;
					}
				}

				for (int k = 0; k < noDims; k++) {
					newCenters[nearestCluster][k] += dataPoints[i][k];
				}
				dataPointCnt[nearestCluster]++;

			}

			for (int i = 0; i < noClusters; i++) {
				for (int j = 0; j < noDims; j++) {
					newCenters[i][j] /= (dataPointCnt[i] != 0 ? dataPointCnt[i] : 1);
				}
			}
		}

		public String getDataPoints() {
			return points2String(this.dataPoints);
		}

		public String getClusterCenters() {
			return points2String(centers);
		}

		public String getNewClusterCenters() {
			return points2String(newCenters);
		}

		private String points2String(double[][] points) {
			StringBuilder sb = new StringBuilder();
			DecimalFormat df = new DecimalFormat("#.00");

			for (int i = 0; i < points.length; i++) {
				sb.append(i);
				sb.append('|');
				for (int j = 0; j < points[i].length; j++) {
					sb.append(df.format(points[i][j]));
					sb.append('|');
				}
				sb.append('\n');
			}
			return sb.toString();
		}

		private double computeDistance(double[] a, double[] b) {
			double sqrdSum = 0.0;
			for (int i = 0; i < a.length; i++) {
				sqrdSum += Math.pow(a[i] - b[i], 2);
			}
			return Math.sqrt(sqrdSum);
		}

	}
}
