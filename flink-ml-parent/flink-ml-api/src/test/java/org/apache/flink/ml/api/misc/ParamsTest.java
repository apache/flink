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

package org.apache.flink.ml.api.misc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Test for the behavior and validator of {@link Params}.
 */
public class ParamsTest {
	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testDefaultBehavior() {
		Params params = new Params();

		ParamInfo<String> optionalWithoutDefault =
			ParamInfoFactory.createParamInfo("a", String.class).build();
		assert params.get(optionalWithoutDefault) == null;

		ParamInfo<String> optionalWithDefault =
			ParamInfoFactory.createParamInfo("a", String.class).setHasDefaultValue("def").build();
		assert params.get(optionalWithDefault).equals("def");

		ParamInfo<String> requiredWithDefault =
			ParamInfoFactory.createParamInfo("a", String.class).setRequired()
				.setHasDefaultValue("def").build();
		assert params.get(requiredWithDefault).equals("def");

		ParamInfo<String> requiredWithoutDefault =
			ParamInfoFactory.createParamInfo("a", String.class).setRequired().build();
		thrown.expect(RuntimeException.class);
		thrown.expectMessage("a not exist which is not optional and don't have a default value");
		params.get(requiredWithoutDefault);
	}

	@Test
	public void testValidator() {
		Params params = new Params();

		ParamInfo<Integer> intParam =
			ParamInfoFactory.createParamInfo("a", Integer.class).setValidator(i -> i > 0).build();
		params.set(intParam, 1);

		thrown.expect(RuntimeException.class);
		thrown.expectMessage("Setting a as a invalid value:0");
		params.set(intParam, 0);
	}
}
