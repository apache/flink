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

package org.apache.flink.api.common.operators.base;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.RuntimeUDFContext;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Collector;
import org.junit.Assert;
import org.junit.Test;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Future;

@SuppressWarnings("serial")
public class FlatMapOperatorCollectionTest implements Serializable {

	@Test
	public void testExecuteOnCollection() {
		try {
			IdRichFlatMap<String> udf = new IdRichFlatMap<String>();
			testExecuteOnCollection(udf, Arrays.asList("f", "l", "i", "n", "k"), true);
			Assert.assertTrue(udf.isClosed);

			udf = new IdRichFlatMap<String>();
			testExecuteOnCollection(udf, Arrays.asList("f", "l", "i", "n", "k"), false);
			Assert.assertTrue(udf.isClosed);
			
			udf = new IdRichFlatMap<String>();
			testExecuteOnCollection(udf, Collections.<String>emptyList(), true);
			Assert.assertTrue(udf.isClosed);
			
			udf = new IdRichFlatMap<String>();
			testExecuteOnCollection(udf, Collections.<String>emptyList(), false);
			Assert.assertTrue(udf.isClosed);
		}
		catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	private void testExecuteOnCollection(FlatMapFunction<String, String> udf, List<String> input, boolean mutableSafe) throws Exception {
		ExecutionConfig executionConfig = new ExecutionConfig();
		if (mutableSafe) {
			executionConfig.disableObjectReuse();
		} else {
			executionConfig.enableObjectReuse();
		}
		final TaskInfo taskInfo = new TaskInfo("Test UDF", 0, 4, 0);
		// run on collections
		final List<String> result = getTestFlatMapOperator(udf)
				.executeOnCollections(input, new RuntimeUDFContext(taskInfo,  null, executionConfig, new HashMap<String, Future<Path>>(), new HashMap<String, Accumulator<?, ?>>()), executionConfig);

		Assert.assertEquals(input.size(), result.size());
		Assert.assertEquals(input, result);
	}


	public class IdRichFlatMap<IN> extends RichFlatMapFunction<IN, IN> {

		private boolean isOpened = false;
		private boolean isClosed = false;

		@Override
		public void open(Configuration parameters) throws Exception {
			isOpened = true;

			RuntimeContext ctx = getRuntimeContext();
			Assert.assertEquals("Test UDF", ctx.getTaskName());
			Assert.assertEquals(4, ctx.getNumberOfParallelSubtasks());
			Assert.assertEquals(0, ctx.getIndexOfThisSubtask());
		}

		@Override
		public void flatMap(IN value, Collector<IN> out) throws Exception {
			Assert.assertTrue(isOpened);
			Assert.assertFalse(isClosed);

			out.collect(value);
		}

		@Override
		public void close() throws Exception {
			isClosed = true;
		}
	}

	private FlatMapOperatorBase<String, String, FlatMapFunction<String, String>> getTestFlatMapOperator(
			FlatMapFunction<String, String> udf) {

		UnaryOperatorInformation<String, String> typeInfo = new UnaryOperatorInformation<String, String>(
				BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);

		return new FlatMapOperatorBase<String, String, FlatMapFunction<String, String>>(
				udf, typeInfo, "flatMap on Collections");
	}
}
