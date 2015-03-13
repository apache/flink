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
import org.apache.flink.api.expressions.tree.EqualTo$;
import org.apache.flink.api.expressions.tree.Expression;
import org.apache.flink.api.expressions.tree.Literal$;
import org.apache.flink.api.expressions.tree.UnresolvedFieldReference$;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.scala.expressions.JavaBatchTranslator;

/**
 * This is extremely bare-bones. We need a parser that can parse expressions in a String
 * and create the correct expression AST. Then we can use expressions like this:
 *
 * {@code in.select("'field0.avg, 'field1.count") }
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
				new WC("Hello", 1)
		);

		ExpressionOperation<JavaBatchTranslator> expr = new JavaBatchTranslator().createExpressionOperation(
				input,
				new Expression[] { UnresolvedFieldReference$.MODULE$.apply("count"), UnresolvedFieldReference$.MODULE$.apply("word")});

		ExpressionOperation<JavaBatchTranslator> filtered = expr.filter(
				EqualTo$.MODULE$.apply(UnresolvedFieldReference$.MODULE$.apply("word"), Literal$.MODULE$.apply("Hello")));

		DataSet<WC> result = (DataSet<WC>) filtered.as(TypeExtractor.createTypeInfo(WC.class));

		result.print();
		env.execute();
	}
}
