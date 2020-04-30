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

package org.apache.flink.graph.drivers.input;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvInputFormat;
import org.apache.flink.client.program.ProgramParametrizationException;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphCsvReader;
import org.apache.flink.graph.drivers.parameter.ChoiceParameter;
import org.apache.flink.graph.drivers.parameter.Simplify;
import org.apache.flink.graph.drivers.parameter.StringParameter;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.apache.flink.types.StringValue;

import org.apache.commons.lang3.text.WordUtils;

/**
 * Read a {@link Graph} from a CSV file using {@link IntValue},
 * {@link LongValue}, or {@link StringValue} keys.
 *
 * @param <K> key type
 */
public class CSV<K extends Comparable<K>>
extends InputBase<K, NullValue, NullValue> {

	private static final String INTEGER = "integer";

	private static final String LONG = "long";

	private static final String STRING = "string";

	private ChoiceParameter type = new ChoiceParameter(this, "type")
		.setDefaultValue(INTEGER)
		.addChoices(LONG, STRING);

	private StringParameter inputFilename = new StringParameter(this, "input_filename");

	private StringParameter commentPrefix = new StringParameter(this, "comment_prefix")
		.setDefaultValue("#");

	private StringParameter lineDelimiter = new StringParameter(this, "input_line_delimiter")
		.setDefaultValue(CsvInputFormat.DEFAULT_LINE_DELIMITER);

	private StringParameter fieldDelimiter = new StringParameter(this, "input_field_delimiter")
		.setDefaultValue(CsvInputFormat.DEFAULT_FIELD_DELIMITER);

	private Simplify simplify = new Simplify(this);

	@Override
	public String getIdentity() {
		return WordUtils.capitalize(getName()) + WordUtils.capitalize(type.getValue()) + " (" + inputFilename + ")";
	}

	@Override
	public Graph<K, NullValue, NullValue> create(ExecutionEnvironment env) throws Exception {
		GraphCsvReader reader = Graph.fromCsvReader(inputFilename.getValue(), env)
			.ignoreCommentsEdges(commentPrefix.getValue())
			.lineDelimiterEdges(lineDelimiter.getValue())
			.fieldDelimiterEdges(fieldDelimiter.getValue());

		Graph<K, NullValue, NullValue> graph;

		switch (type.getValue()) {
			case INTEGER:
				graph = (Graph<K, NullValue, NullValue>) reader
					.keyType(IntValue.class);
				break;

			case LONG:
				graph = (Graph<K, NullValue, NullValue>) reader
					.keyType(LongValue.class);
				break;

			case STRING:
				graph = (Graph<K, NullValue, NullValue>) reader
					.keyType(StringValue.class);
				break;

			default:
				throw new ProgramParametrizationException("Unknown type '" + type.getValue() + "'");
		}

		return simplify.simplify(graph, parallelism.getValue().intValue());
	}
}
