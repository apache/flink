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
package org.apache.flink.examples.java;


import org.apache.flink.api.expressions.ExpressionOperation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.expressions.ExpressionUtil;

/**
 * Very simple example that shows how the Java Expression API can be used.
 */
public class JavaExpressionExample {

	public static class WC {
		public String word;
		public int count;

		public WC() {

		}

		public WC(String word, int count) {
			this.word = word;
			this.count = count;
		}

		@Override
		public String toString() {
			return "WC " + word + " " + count;
		}
	}
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();

		DataSet<WC> input = env.fromElements(
				new WC("Hello", 1),
				new WC("Ciao", 1),
				new WC("Hello", 1));

		ExpressionOperation expr = ExpressionUtil.from(input);

		ExpressionOperation filtered = expr
				.groupBy("word")
				.select("word.count as count, word")
				.filter("count = 2");

		DataSet<WC> result = ExpressionUtil.toSet(filtered, WC.class);

		result.print();
		env.execute();
	}
}
