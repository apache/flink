/*
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

package org.apache.flink.test.io;

import org.apache.flink.api.common.operators.util.TestNonRichInputFormat;
import org.apache.flink.api.common.operators.util.TestNonRichOutputFormat;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.test.util.JavaProgramTestBase;

import static org.junit.Assert.fail;

/**
 * Tests for non rich DataSource and DataSink input output formats being correctly used at runtime.
 */
public class InputOutputITCase extends JavaProgramTestBase {

	@Override
	protected void testProgram() throws Exception {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TestNonRichOutputFormat output = new TestNonRichOutputFormat();
		env.createInput(new TestNonRichInputFormat()).output(output);
		try {
			env.execute();
		} catch (Exception e){
			// we didn't break anything by making everything rich.
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
