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

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import org.apache.flink.api.common.io.GenericCsvInputFormat;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.types.parser.FieldParser;

import java.io.IOException;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.StringUtils;

public abstract class CsvInputFormat<OUT> extends GenericCsvInputFormat<OUT> {

	private static final long serialVersionUID = 1L;

	public static final String DEFAULT_LINE_DELIMITER = "\n";

	public static final String DEFAULT_FIELD_DELIMITER = ",";

	protected transient Object[] parsedValues;
	
	protected CsvInputFormat(Path filePath) {
		super(filePath);
	}

	@Override
	public void open(FileInputSplit split) throws IOException {
		super.open(split);

		@SuppressWarnings("unchecked")
		FieldParser<Object>[] fieldParsers = (FieldParser<Object>[]) getFieldParsers();

		//throw exception if no field parsers are available
		if (fieldParsers.length == 0) {
			throw new IOException("CsvInputFormat.open(FileInputSplit split) - no field parsers to parse input");
		}

		// create the value holders
		this.parsedValues = new Object[fieldParsers.length];
		for (int i = 0; i < fieldParsers.length; i++) {
			this.parsedValues[i] = fieldParsers[i].createValue();
		}

		// left to right evaluation makes access [0] okay
		// this marker is used to fasten up readRecord, so that it doesn't have to check each call if the line ending is set to default
		if (this.getDelimiter().length == 1 && this.getDelimiter()[0] == '\n' ) {
			this.lineDelimiterIsLinebreak = true;
		}

		this.commentCount = 0;
		this.invalidLineCount = 0;
	}

	@Override
	public OUT nextRecord(OUT record) throws IOException {
		OUT returnRecord = null;
		do {
			returnRecord = super.nextRecord(record);
		} while (returnRecord == null && !reachedEnd());

		return returnRecord;
	}

	public Class<?>[] getFieldTypes() {
		return super.getGenericFieldTypes();
	}

	protected static boolean[] createDefaultMask(int size) {
		boolean[] includedMask = new boolean[size];
		for (int x=0; x<includedMask.length; x++) {
			includedMask[x] = true;
		}
		return includedMask;
	}

	protected static boolean[] toBooleanMask(int[] sourceFieldIndices) {
		Preconditions.checkNotNull(sourceFieldIndices);

		for (int i : sourceFieldIndices) {
			if (i < 0) {
				throw new IllegalArgumentException("Field indices must not be smaller than zero.");
			}
		}

		boolean[] includedMask = new boolean[Ints.max(sourceFieldIndices) + 1];

		// check if we support parsers for these types
		for (int i = 0; i < sourceFieldIndices.length; i++) {
			includedMask[sourceFieldIndices[i]] = true;
		}

		return includedMask;
	}

	@Override
	public String toString() {
		return "CSV Input (" + StringUtils.showControlCharacters(String.valueOf(getFieldDelimiter())) + ") " + getFilePath();
	}
	
}
