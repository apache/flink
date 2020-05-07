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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.io.GenericCsvInputFormat;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.parser.FieldParser;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import java.io.IOException;
import java.util.Arrays;

/**
 * InputFormat that reads csv files.
 *
 * @param <OUT>
 */
@Internal
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

		// create the value holders
		this.parsedValues = new Object[fieldParsers.length];
		for (int i = 0; i < fieldParsers.length; i++) {
			this.parsedValues[i] = fieldParsers[i].createValue();
		}

		// left to right evaluation makes access [0] okay
		// this marker is used to fasten up readRecord, so that it doesn't have to check each call if the line ending is set to default
		if (this.getDelimiter().length == 1 && this.getDelimiter()[0] == '\n') {
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

	@Override
	public OUT readRecord(OUT reuse, byte[] bytes, int offset, int numBytes) throws IOException {
		/*
		 * Fix to support windows line endings in CSVInputFiles with standard delimiter setup = \n
		 */
		// Found window's end line, so find carriage return before the newline
		if (this.lineDelimiterIsLinebreak && numBytes > 0 && bytes[offset + numBytes - 1] == '\r') {
			//reduce the number of bytes so that the Carriage return is not taken as data
			numBytes--;
		}

		if (commentPrefix != null && commentPrefix.length <= numBytes) {
			//check record for comments
			boolean isComment = true;
			for (int i = 0; i < commentPrefix.length; i++) {
				if (commentPrefix[i] != bytes[offset + i]) {
					isComment = false;
					break;
				}
			}
			if (isComment) {
				this.commentCount++;
				return null;
			}
		}

		if (parseRecord(parsedValues, bytes, offset, numBytes)) {
			return fillRecord(reuse, parsedValues);
		} else {
			this.invalidLineCount++;
			return null;
		}
	}

	protected abstract OUT fillRecord(OUT reuse, Object[] parsedValues);

	public Class<?>[] getFieldTypes() {
		return super.getGenericFieldTypes();
	}

	protected static boolean[] createDefaultMask(int size) {
		boolean[] includedMask = new boolean[size];
		for (int x = 0; x < includedMask.length; x++) {
			includedMask[x] = true;
		}
		return includedMask;
	}

	protected static boolean[] toBooleanMask(int[] sourceFieldIndices) {
		Preconditions.checkNotNull(sourceFieldIndices);

		int max = 0;
		for (int i : sourceFieldIndices) {
			if (i < 0) {
				throw new IllegalArgumentException("Field indices must not be smaller than zero.");
			}
			max = Math.max(i, max);
		}

		boolean[] includedMask = new boolean[max + 1];

		// check if we support parsers for these types
		for (int i = 0; i < sourceFieldIndices.length; i++) {
			includedMask[sourceFieldIndices[i]] = true;
		}

		return includedMask;
	}

	@Override
	public String toString() {
		return "CSV Input (" + StringUtils.showControlCharacters(String.valueOf(getFieldDelimiter())) + ") " + Arrays.toString(getFilePaths());
	}

}
