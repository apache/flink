/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.test.example.scala;

import org.apache.flink.examples.scala.graph.EnumTriangles;
import org.apache.flink.test.testdata.EnumTriangleData;
import org.apache.flink.test.util.JavaProgramTestBase;

/**
 * Test {@link EnumTriangles}.
 */
public class EnumTriangleITCase extends JavaProgramTestBase {

	protected String edgePath;
	protected String resultPath;

	@Override
	protected void preSubmit() throws Exception {
		edgePath = createTempFile("edges", EnumTriangleData.EDGES);
		resultPath = getTempDirPath("triangles");
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(EnumTriangleData.TRIANGLES_BY_ID, resultPath);
	}

	@Override
	protected void testProgram() throws Exception {
		EnumTriangles.main(new String[] {
				"--edges", edgePath,
				"--output", resultPath });
	}

}
