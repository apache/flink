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

package org.apache.flink.api.java.aggregation;

import static org.apache.flink.util.TestHelper.uniqueInt;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.apache.flink.api.java.tuple.Tuple;
import org.junit.Test;

public class AggregationUdfBaseTest {

	private AggregationUdfBase<Tuple> udf;
	
	@Test
	public void shouldCreateResultTuple() {
		// given
		int arity = uniqueInt(1, Tuple.MAX_ARITY + 1);
		udf = new AggregationUdfBase<Tuple>(arity);

		// when
		Tuple result = udf.createResultTuple();

		// then
		assertThat(result.getArity(), is(arity));
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void errorIfResultTupleTooLarge() {
		// given
		int arity = uniqueInt(Tuple.MAX_ARITY + 1, Tuple.MAX_ARITY + 10);
		
		// when
		udf = new AggregationUdfBase<Tuple>(arity);
	}

}
