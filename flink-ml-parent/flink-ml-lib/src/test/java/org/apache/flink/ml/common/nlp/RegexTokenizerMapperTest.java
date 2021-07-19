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
import org.apache.flink.ml.params.nlp.RegexTokenizerParams;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for RegexTokenizerMapper.
 */
public class RegexTokenizerMapperTest {
	@Test
	public void test1() throws Exception {
		TableSchema schema = new TableSchema(new String[]{"sentence"}, new TypeInformation<?>[]{Types.STRING});

		Params params = new Params()
			.set(RegexTokenizerParams.SELECTED_COL, "sentence");

		RegexTokenizerMapper mapper = new RegexTokenizerMapper(schema, params);

		assertEquals(mapper.map(Row.of("This is a unit test for mapper")).getField(0),
			"this is a unit test for mapper");
		assertEquals(mapper.getOutputSchema(), schema);
	}

	@Test
	public void test2() throws Exception {
		TableSchema schema = new TableSchema(new String[]{"sentence"}, new TypeInformation<?>[]{Types.STRING});

		Params params = new Params()
			.set(RegexTokenizerParams.SELECTED_COL, "sentence")
			.set(RegexTokenizerParams.MIN_TOKEN_LENGTH, 3);

		RegexTokenizerMapper mapper = new RegexTokenizerMapper(schema, params);

		assertEquals(mapper.map(Row.of("This is a unit test for mapper")).getField(0), "this unit test for mapper");
		assertEquals(mapper.getOutputSchema(), schema);
	}

	@Test
	public void test3() throws Exception {
		TableSchema schema = new TableSchema(new String[]{"sentence"}, new TypeInformation<?>[]{Types.STRING});

		Params params = new Params()
			.set(RegexTokenizerParams.SELECTED_COL, "sentence")
			.set(RegexTokenizerParams.GAPS, false)
			.set(RegexTokenizerParams.PATTERN, "\\W");

		RegexTokenizerMapper mapper = new RegexTokenizerMapper(schema, params);

		assertEquals(mapper.map(Row.of("This is a unit test for mapper!")).getField(0), "            !");
		assertEquals(mapper.getOutputSchema(), schema);
	}

	@Test
	public void test4() throws Exception {
		TableSchema schema = new TableSchema(new String[]{"sentence"}, new TypeInformation<?>[]{Types.STRING});

		Params params = new Params()
			.set(RegexTokenizerParams.SELECTED_COL, "sentence")
			.set(RegexTokenizerParams.TO_LOWER_CASE, true);

		RegexTokenizerMapper mapper = new RegexTokenizerMapper(schema, params);

		assertEquals(mapper.map(Row.of("This is a unit test for mapper")).getField(0),
			"this is a unit test for mapper");
		assertEquals(mapper.getOutputSchema(), schema);
	}

}
