/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.api.java.io;

import eu.stratosphere.api.common.io.FileOutputFormat;
import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.types.StringValue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;

/**
 * This is an OutputFormat to serialize {@link eu.stratosphere.api.java.tuple.Tuple}s to text. The output is
 * structured by record delimiters and field delimiters as common in CSV files.
 * Record delimiter separate records from each other ('\n' is common). Field
 * delimiters separate fields within a record.
 */
public class CsvOutputFormat<T extends Tuple> extends FileOutputFormat<T> {
	private static final long serialVersionUID = 1L;

	@SuppressWarnings("unused")
	private static final Log LOG = LogFactory.getLog(CsvOutputFormat.class);

	// --------------------------------------------------------------------------------------------

	private Writer wrt;

	private String fieldDelimiter;

	private String recordDelimiter;

	private String charsetName;

	private boolean lenient;

	private boolean quoteStrings = false;

	// --------------------------------------------------------------------------------------------
	// Constructors and getters/setters for the configurable parameters
	// --------------------------------------------------------------------------------------------

	/**
	 * Creates an instance of CsvOutputFormat. As the default value for separating records '\n' is
	 * used. The default field delimiter is ','.
	 */
	public CsvOutputFormat(Path outputPath) {
		this(outputPath, "\n", ",");
	}

	/**
	 * Creates an instance of CsvOutputFormat. As the default value for separating
	 * records '\n' is used.
	 * 
	 * @param fieldDelimiter
	 *            The delimiter that is used to separate the different fields in
	 *            the record.
	 */
	public CsvOutputFormat(Path outputPath, String fieldDelimiter) {
		this(outputPath, "\n", fieldDelimiter);
	}

	/**
	 * Creates an instance of CsvOutputFormat.
	 * 
	 * @param recordDelimiter
	 *            The delimiter that is used to separate the different records.
	 * @param fieldDelimiter
	 *            The delimiter that is used to separate the different fields in
	 *            the record.
	 */
	public CsvOutputFormat(Path outputPath, String recordDelimiter, String fieldDelimiter) {
		super(outputPath);
		if (recordDelimiter == null) {
			throw new IllegalArgumentException("RecordDelmiter shall not be null.");
		}
		if (fieldDelimiter == null) {
			throw new IllegalArgumentException("FieldDelimiter shall not be null.");
		}

		this.fieldDelimiter = fieldDelimiter;
		this.recordDelimiter = recordDelimiter;
		this.lenient = false;
	}
	
	public void setLenient(boolean lenient) {
		this.lenient = lenient;
	}

	public void setCharsetName(String charsetName) {
		this.charsetName = charsetName;
	}

	public void setQuoteStrings(boolean quoteStrings) {
		this.quoteStrings = quoteStrings;
	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException
	{
		super.open(taskNumber, numTasks);
		this.wrt = this.charsetName == null ? new OutputStreamWriter(new BufferedOutputStream(this.stream, 4096)) :
				new OutputStreamWriter(new BufferedOutputStream(this.stream, 4096), this.charsetName);
	}

	@Override
	public void close() throws IOException {
		if (wrt != null) {
			this.wrt.close();
		}
		super.close();
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public void writeRecord(T element) throws IOException {
		int numFields = element.getArity();

		for (int i = 0; i < numFields; i++) {
			Object v = element.getFieldFast(i);
			if (v != null) {
				if (i != 0) {
					this.wrt.write(this.fieldDelimiter);
				}

				if (quoteStrings) {
					if (v instanceof String || v instanceof StringValue) {
						this.wrt.write("\"");
						this.wrt.write(v.toString());
						this.wrt.write("\"");
					} else {
						this.wrt.write(v.toString());
					}
				} else {
					this.wrt.write(v.toString());
				}
			} else {
				if (this.lenient) {
					if (i != 0) {
						this.wrt.write(this.fieldDelimiter);
					}
				} else {
					throw new RuntimeException("Cannot serialize tuple with <null> value at position: " + i);
				}
			}
		}

		// add the record delimiter
		this.wrt.write(this.recordDelimiter);
	}
}
