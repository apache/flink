/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.common.nlp;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.params.nlp.NGramParams;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for NGramMapper.
 */
public class NGramMapperTest {
	@Test
	public void test1() throws Exception {
		TableSchema schema = new TableSchema(new String[]{"sentence"}, new TypeInformation<?>[]{Types.STRING});

		Params params = new Params()
			.set(NGramParams.SELECTED_COL, "sentence");

		NGramMapper mapper = new NGramMapper(schema, params);

		assertEquals(mapper.map(Row.of("This is a unit test for mapper")).getField(0),
			"This_is is_a a_unit unit_test test_for for_mapper");
		assertEquals(mapper.getOutputSchema(), schema);
	}

	@Test
	public void test2() throws Exception {
		TableSchema schema = new TableSchema(new String[]{"sentence"}, new TypeInformation<?>[]{Types.STRING});

		Params params = new Params()
			.set(NGramParams.SELECTED_COL, "sentence")
			.set(NGramParams.N, 3);

		NGramMapper mapper = new NGramMapper(schema, params);

		assertEquals(mapper.map(Row.of("This is a unit test for mapper")).getField(0),
			"This_is_a is_a_unit a_unit_test unit_test_for test_for_mapper");
		assertEquals(mapper.getOutputSchema(), schema);
	}

	@Test
	public void test3() throws Exception {
		TableSchema schema = new TableSchema(new String[]{"sentence"}, new TypeInformation<?>[]{Types.STRING});

		Params params = new Params()
			.set(NGramParams.SELECTED_COL, "sentence")
			.set(NGramParams.N, 10);

		NGramMapper mapper = new NGramMapper(schema, params);

		assertEquals(mapper.map(Row.of("This is a unit test for mapper")).getField(0), "");
		assertEquals(mapper.getOutputSchema(), schema);
	}
}
