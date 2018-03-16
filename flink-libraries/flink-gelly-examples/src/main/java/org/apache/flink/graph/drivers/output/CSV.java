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

package org.apache.flink.graph.drivers.output;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.CsvOutputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.graph.drivers.parameter.StringParameter;

import java.io.PrintStream;

/**
 * Write algorithm output to file using CSV format.
 *
 * @param <T> result Type
 */
public class CSV<T>
extends OutputBase<T> {

	private StringParameter filename = new StringParameter(this, "output_filename");

	private StringParameter lineDelimiter = new StringParameter(this, "output_line_delimiter")
		.setDefaultValue(CsvOutputFormat.DEFAULT_LINE_DELIMITER);

	private StringParameter fieldDelimiter = new StringParameter(this, "output_field_delimiter")
		.setDefaultValue(CsvOutputFormat.DEFAULT_FIELD_DELIMITER);

	@Override
	public void write(String executionName, PrintStream out, DataSet<T> data) throws Exception {
		if (Tuple.class.isAssignableFrom(data.getType().getTypeClass())) {
			data
				.writeAsCsv(filename.getValue(), lineDelimiter.getValue(), fieldDelimiter.getValue())
					.name("CSV: " + filename.getValue());
		} else {
			// line and field delimiters are ineffective when writing custom POJOs result types
			data
				.writeAsText(filename.getValue())
					.name("CSV: " + filename.getValue());
		}

		data.getExecutionEnvironment().execute();
	}
}
