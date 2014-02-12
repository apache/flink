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

package eu.stratosphere.test.exampleRecordPrograms;

//import java.io.BufferedReader;
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.Comparator;
//import java.util.StringTokenizer;
//
//import org.junit.Assert;
//
//import eu.stratosphere.api.common.Plan;
//import eu.stratosphere.example.java.record.kmeans.KMeansSingleStep;
//import eu.stratosphere.test.testdata.KMeansData;
//import eu.stratosphere.test.util.TestBase2;
//
//public class KMeansStepITCase extends TestBase2 {
//
//	protected String dataPath;
//	protected String clusterPath;
//	protected String resultPath;
//
//	
//	protected String getNewCenters() {
//		return KMeansData.CENTERS_AFTER_ONE_STEP;
//	}
//
//	@Override
//	protected void preSubmit() throws Exception {
//		dataPath = createTempFile("datapoints.txt", KMeansData.DATAPOINTS);
//		clusterPath = createTempFile("initial_centers.txt", KMeansData.INITIAL_CENTERS);
//		resultPath = getTempDirPath("result");
//	}
//	
//	@Override
//	protected Plan getTestJob() {
//		KMeansSingleStep kmi = new KMeansSingleStep();
//		return kmi.getPlan("4", dataPath, clusterPath, resultPath);
//	}
//
//
//	@Override
//	protected void postSubmit() throws Exception {
//
//		final double MAX_DELTA = 0.1;
//		
//		Comparator<String> deltaComp = new Comparator<String>() {
//
//			@Override
//			public int compare(String o1, String o2) {
//				
//				StringTokenizer st1 = new StringTokenizer(o1, "|");
//				StringTokenizer st2 = new StringTokenizer(o2, "|");
//				
//				if(st1.countTokens() != st2.countTokens()) {
//					return st1.countTokens() - st2.countTokens();
//				}
//				
//				// first token is ID
//				String t1 = st1.nextToken();
//				String t2 = st2.nextToken();
//				if(!t1.equals(t2)) {
//					return t1.compareTo(t2);
//				}
//				
//				while(st1.hasMoreTokens()) {
//					t1 = st1.nextToken();
//					t2 = st2.nextToken();
//					
//					double d1 = Double.parseDouble(t1);
//					double d2 = Double.parseDouble(t2);
//					
//					if (Math.abs(d1-d2) > MAX_DELTA) {
//						return d1 < d2 ? -1 : 1;
//					}
//				}
//				
//				return 0;
//			}
//		};
//		
//		// ------- Test results -----------
//		
//		// Determine all result files
//
//
//		// collect lines of all result files
//		ArrayList<String> resultLines = new ArrayList<String>();
//		for (BufferedReader reader : getResultReader(resultPath)) {
//			String line = null;
//			while ((line = reader.readLine()) != null) {
//				resultLines.add(line);
//			}
//			reader.close();
//		}
//		
//		Collections.sort(resultLines, deltaComp);
//		
//		final String[] should = getNewCenters().split("\n");
//		final String[] is = (String[]) resultLines.toArray(new String[resultLines.size()]);
//		
//		Assert.assertEquals("Wrong number of result lines.", should.length, is.length);
//		
//		for (int i = 0; i < should.length; i++) {
//			StringTokenizer shouldRecord = new StringTokenizer(should[i], "|");
//			StringTokenizer isRecord = new StringTokenizer(is[i], "|");
//			
//			Assert.assertEquals("Records don't match.", shouldRecord.countTokens(), isRecord.countTokens());
//			
//			// first token is ID
//			String shouldToken = shouldRecord.nextToken();
//			String isToken = isRecord.nextToken();
//			
//			Assert.assertEquals("Records don't match.", shouldToken, isToken);
//
//			while (shouldRecord.hasMoreTokens()) {
//				shouldToken = shouldRecord.nextToken();
//				isToken = isRecord.nextToken();
//				
//				double shouldDouble = Double.parseDouble(shouldToken);
//				double isDouble = Double.parseDouble(isToken);
//				
//				Assert.assertTrue(shouldDouble - MAX_DELTA < isDouble && shouldDouble + MAX_DELTA > isDouble);
//			}
//		}
//	}
//}
