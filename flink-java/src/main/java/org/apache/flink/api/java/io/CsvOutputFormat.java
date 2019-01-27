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

package org.apache.flink.api.java.io;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.core.fs.Path;

/**
 * This is an OutputFormat to serialize {@link org.apache.flink.api.java.tuple.Tuple}s to text. The output is
 * structured by record delimiters and field delimiters as common in CSV files.
 * Record delimiter separate records from each other ('\n' is common). Field
 * delimiters separate fields within a record.
 */
@PublicEvolving
public class CsvOutputFormat<T extends Tuple> extends AbstractCsvOutputFormat<T> implements InputTypeConfigurable {

	// --------------------------------------------------------------------------------------------

	public static final String DEFAULT_LINE_DELIMITER = AbstractCsvOutputFormat.DEFAULT_LINE_DELIMITER;

	public static final String DEFAULT_FIELD_DELIMITER = AbstractCsvOutputFormat.DEFAULT_FIELD_DELIMITER;

	// --------------------------------------------------------------------------------------------

	public CsvOutputFormat(Path outputPath) {
		super(outputPath);
	}

	public CsvOutputFormat(Path outputPath, String fieldDelimiter) {
		super(outputPath, fieldDelimiter);
	}

	public CsvOutputFormat(Path outputPath, String recordDelimiter, String fieldDelimiter) {
		super(outputPath, recordDelimiter, fieldDelimiter);
	}

	@Override
	protected Object getSpecificField(T record, int n) {
		return record.getField(n);
	}

	@Override
	protected int getFieldsNum(T record) {
		return record.getArity();
	}

	/**
	 * The purpose of this method is solely to check whether the data type to be processed
	 * is in fact a tuple type.
	 */
	@Override
	public void setInputType(TypeInformation<?> type, ExecutionConfig executionConfig) {
		if (!type.isTupleType()) {
			throw new InvalidProgramException("The " + CsvOutputFormat.class.getSimpleName() +
				" can only be used to write tuple data sets.");
		}
	}
}
