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

package org.apache.flink.table.sources.decorator;

import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * LookupFunctionInvokerTest.
 */
public class LookupFunctionInvokerTest {
	@Test
	public void testNormal() {
		AtomicInteger vaviableObjectCount = new AtomicInteger(0);
		TableFunction<Row> functionMulit = new TableFunction<Row>() {
			public void eval(Object... keys) {
				vaviableObjectCount.incrementAndGet();
				System.out.println(Arrays.asList(keys));
			}
		};

		LookupFunctionInvoker invokerMulti = new LookupFunctionInvoker(functionMulit);
		LookupFunctionInvoker.Evaluation evaluationMulti = invokerMulti.getProxy();
		evaluationMulti.eval("1", 2, "3");
		evaluationMulti.eval(2L, 3L);
		evaluationMulti.eval(2L, 3L, 4L);
		evaluationMulti.eval(5L, 3L, 4L, 7L);
		evaluationMulti.eval(10, 11, 12, 13, 14);
		evaluationMulti.eval(10, 11, 12, 13, 14);
		evaluationMulti.eval(10, 11, 12, 13, 14);
		evaluationMulti.eval(10, 11, 12, 13, 14);
		evaluationMulti.eval(10, 11, 12, 13, 14);
		evaluationMulti.eval("1", "2", "3");
		evaluationMulti.eval("1", "2", "4");
		evaluationMulti.eval("1", "2", "5");
		evaluationMulti.eval("1", "2", "6");
		evaluationMulti.eval("1", "2", "7");
		evaluationMulti.eval("1", "2", "7");
		evaluationMulti.eval("1", "2", "7");
		Assert.assertEquals(16, vaviableObjectCount.get());

		AtomicInteger singleObjectCount = new AtomicInteger(0);
		TableFunction<Row> functionSingle = new TableFunction<Row>() {
			public void eval(Object key) {
				singleObjectCount.incrementAndGet();
				System.out.println(Arrays.asList(key));
			}
		};

		LookupFunctionInvoker invoker = new LookupFunctionInvoker(functionSingle);
		LookupFunctionInvoker.Evaluation evaluationSingle = invoker.getProxy();
		evaluationSingle.eval(new Object());
		evaluationSingle.eval(new Object());
		evaluationSingle.eval(3L);
		evaluationSingle.eval(4L);
		evaluationSingle.eval(5L);
		evaluationSingle.eval(6L);
		evaluationSingle.eval(15);
		evaluationSingle.eval(16);
		evaluationSingle.eval(17);
		evaluationSingle.eval(18);
		evaluationSingle.eval(18);
		evaluationSingle.eval(18);
		evaluationSingle.eval("1");
		evaluationSingle.eval("2");
		evaluationSingle.eval("3");
		evaluationSingle.eval("4");
		evaluationSingle.eval("5");
		evaluationSingle.eval("6");
		evaluationSingle.eval("7");
		evaluationSingle.eval("78");
		Assert.assertEquals(20, singleObjectCount.get());
	}
}
