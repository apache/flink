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
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.Row;

/**
 * This is an OutputFormat to serialize {@link Row}s to text. The output is
 * structured by record delimiters and field delimiters as common in CSV files.
 * Record delimiter separate records from each other ('\n' is common). Field
 * delimiters separate fields within a record.
 */
@PublicEvolving
public class RowCsvOutputFormat extends AbstractCsvOutputFormat<Row> {

	public RowCsvOutputFormat(Path outputPath) {
		super(outputPath);
	}

	public RowCsvOutputFormat(Path outputPath, String fieldDelimiter) {
		super(outputPath, fieldDelimiter);
	}

	public RowCsvOutputFormat(Path outputPath, String recordDelimiter, String fieldDelimiter) {
		super(outputPath, recordDelimiter, fieldDelimiter);
	}

	public RowCsvOutputFormat(Path outputPath, String recordDelimiter, String fieldDelimiter, String quoteCharacter) {
		super(outputPath, recordDelimiter, fieldDelimiter, quoteCharacter);
	}

	public RowCsvOutputFormat(Path outputPath, String recordDelimiter, String fieldDelimiter, char quoteCharacter) {
		super(outputPath, recordDelimiter, fieldDelimiter, quoteCharacter);
	}

	@Override
	protected Object getSpecificField(Row record, int n) {
		return record.getField(n);
	}

	@Override
	protected int getFieldsNum(Row record) {
		return record.getArity();
	}
}
