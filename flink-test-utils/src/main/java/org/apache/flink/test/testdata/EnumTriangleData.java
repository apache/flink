/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.testdata;

public class EnumTriangleData {

	public static final String EDGES = 
			"1 2\n" +
			"1 3\n" +
			"1 4\n" +
			"1 5\n" +
			"2 3\n" +
			"2 5\n" +
			"3 4\n" +
			"3 7\n" +
			"5 6\n" +
			"3 8\n" +
			"7 8\n";

	public static final String TRIANGLES_BY_ID = 
			"1,2,3\n" +
			"1,3,4\n" +
			"1,2,5\n" +
			"3,7,8\n"; 
	
	public static final String TRIANGLES_BY_DEGREE = 
			"2,1,3\n" +
			"4,1,3\n" +
			"2,1,5\n" +
			"7,3,8\n";

}
