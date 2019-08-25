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

package org.apache.flink.ml.util.param;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.api.misc.param.WithParams;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Test for {@link ExtractParamInfosUtil}.
 */
public class ExtractParamInfosUtilTest {

	@Test
	public void testExtractParamInfos() {
		List<ParamInfo> noParamInfos =
			ExtractParamInfosUtil.extractParamInfos(new WithNoParamInfo());
		assert noParamInfos.isEmpty();

		List<ParamInfo> classParamInfos =
			ExtractParamInfosUtil.extractParamInfos(new WithTestParamInfo());
		assert classParamInfos.size() == 1 && classParamInfos.get(0).getName().equals("KSC");

		List<ParamInfo> allParamInfos =
			ExtractParamInfosUtil.extractParamInfos(new TestParamInfoWithInheritedParamInfos());
		String[] sortedCorrectParamNames = new String[]{"KCP", "KI", "KSC"};
		assert allParamInfos.size() == 3 && Arrays.equals(sortedCorrectParamNames,
			allParamInfos.stream().map(ParamInfo::getName).sorted().toArray(String[]::new));
	}

	/**
	 * Mock WithParams implementation with no ParamInfo. Only for test.
	 */
	public static class WithNoParamInfo implements WithParams<WithNoParamInfo> {

		@Override
		public Params getParams() {
			return null;
		}
	}

	/**
	 * Mock WithParams implementation with one ParamInfo. Only for test.
	 * @param <T> subclass of WithTestParamInfo
	 */
	public static class WithTestParamInfo<T extends WithTestParamInfo> implements WithParams<T> {
		public static final ParamInfo<String> KSC = ParamInfoFactory
			.createParamInfo("KSC", String.class)
			.setDescription("key from super class").build();

		@Override
		public Params getParams() {
			return null;
		}
	}

	/**
	 * Mock interface extending WithParams with one ParamInfo. Only for test.
	 * @param <T> implementation class of InterfaceWithParamInfo
	 */
	public interface InterfaceWithParamInfo<T extends InterfaceWithParamInfo>
		extends WithParams<T> {
		ParamInfo<String> KI = ParamInfoFactory.createParamInfo("KI", String.class)
			.setDescription("key from interface").build();
	}

	/**
	 * Mock WithParams inheriting ParamInfos from superclass and interface. Only for test.
	 */
	public static class TestParamInfoWithInheritedParamInfos
		extends WithTestParamInfo<TestParamInfoWithInheritedParamInfos>
		implements InterfaceWithParamInfo<TestParamInfoWithInheritedParamInfos> {
		private static final ParamInfo<String> KCP = ParamInfoFactory
			.createParamInfo("KCP", String.class)
			.setDescription("key in the class which is private").build();

		@Override
		public Params getParams() {
			return null;
		}
	}
}
