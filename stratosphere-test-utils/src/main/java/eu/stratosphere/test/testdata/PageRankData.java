/***********************************************************************************************************************
 *
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
 *
 **********************************************************************************************************************/

package eu.stratosphere.test.testdata;

public class PageRankData {

	public static final int NUM_VERTICES = 5;
	
	public static final String VERTICES = 	"1\n" +
											"2\n" +
											"5\n" +
											"3\n" +
											"4";
	
	public static final String EDGES = "2 1\n" +
										"5 2\n" + 
										"5 4\n" +
										"4 3\n" +
										"4 2\n" +
										"1 4\n" +
										"1 2\n" +
										"1 3\n" +
										"3 5\n";

	
	public static final String RANKS_AFTER_3_ITERATIONS = "1 0.237\n" +
														"2 0.248\n" + 
														"3 0.173\n" +
														"4 0.175\n" +
														"5 0.165";

	
	public static final String RANKS_AFTER_EPSILON_0_0001_CONVERGENCE = "1 0.238\n" +
																		"2 0.244\n" +
																		"3 0.170\n" +
																		"4 0.171\n" +
																		"5 0.174";
	
	private PageRankData() {}
}
