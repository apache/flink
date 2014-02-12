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
package eu.stratosphere.test.localDistributed;

import java.io.File;
import java.io.FileWriter;
import java.net.URL;
import java.util.LinkedList;

import org.junit.Assert;
import org.junit.Test;

import eu.stratosphere.client.RemoteExecutor;
import eu.stratosphere.client.localDistributed.LocalDistributedExecutor;
import eu.stratosphere.test.testdata.KMeansData;

// When the API changes WordCountForTest needs to be rebuilt and the WordCountForTest.jar in resources needs
// to be replaced with the new one.

public class PackagedProgramEndToEndTest {

	@Test
	public void testEverything() {
		LocalDistributedExecutor lde = new LocalDistributedExecutor();
		try {
			// set up the files
			File points = File.createTempFile("kmeans_points", ".in");
			File clusters = File.createTempFile("kmeans_clusters", ".in");
			File outFile = File.createTempFile("kmeans_result", ".out");
			points.deleteOnExit();
			clusters.deleteOnExit();
			outFile.deleteOnExit();
			outFile.delete();
			
			FileWriter fwPoints = new FileWriter(points);
			fwPoints.write(KMeansData.DATAPOINTS);
			fwPoints.close();

			FileWriter fwClusters = new FileWriter(clusters);
			fwClusters.write(KMeansData.INITIAL_CENTERS);
			fwClusters.close();

			URL jarFileURL = getClass().getResource("/KMeansForTest.jar");
			String jarPath = jarFileURL.getFile();

			// run WordCount

			lde.start(2);
			RemoteExecutor ex = new RemoteExecutor("localhost", 6498, new LinkedList<String>());

			ex.executeJar(jarPath,
					"eu.stratosphere.examples.scala.datamining.KMeansForTest",
					new String[] {"4",
							points.toURI().toString(),
							clusters.toURI().toString(),
							outFile.toURI().toString(),
							"1"});

			points.delete();
			clusters.delete();
			outFile.delete();
			
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		} finally {
			try {
				lde.stop();
			} catch (Exception e) {
				e.printStackTrace();
				Assert.fail(e.getMessage());
			}
		}
	}
}
