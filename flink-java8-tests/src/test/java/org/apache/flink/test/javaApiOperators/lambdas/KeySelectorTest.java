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

package org.apache.flink.test.javaApiOperators.lambdas;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.functions.UnsupportedLambdaExpressionException;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.BasicTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.junit.Assert;

public class KeySelectorTest {

	public void testSelectorLambda() {
		try {
			KeySelector<Tuple2<String, Integer>, String> selector = (t) -> t.f0;
			
			try {
				TypeExtractor.getKeySelectorTypes(selector, 
						new TupleTypeInfo<Tuple2<String, Integer>>(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO));
				Assert.fail("No unsupported lambdas exception");
			}
			catch (UnsupportedLambdaExpressionException e) {
				// good
			}
			catch (Exception e) {
				Assert.fail("Wrong exception type");
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}
}
