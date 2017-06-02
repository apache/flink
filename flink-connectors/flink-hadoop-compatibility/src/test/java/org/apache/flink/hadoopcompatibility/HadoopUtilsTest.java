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

package org.apache.flink.hadoopcompatibility;

import org.apache.flink.api.java.utils.AbstractParameterToolTest;
import org.apache.flink.api.java.utils.ParameterTool;

import org.junit.Test;

import java.io.IOException;

/**
 * Tests for the {@link HadoopUtils}.
 */
public class HadoopUtilsTest extends AbstractParameterToolTest {

	@Test
	public void testParamsFromGenericOptionsParser() throws IOException {
		ParameterTool parameter = HadoopUtils.paramsFromGenericOptionsParser(new String[]{"-D", "input=myInput", "-DexpectedCount=15"});
		validate(parameter);
	}
}
