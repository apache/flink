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
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.StringValue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

/**
 * This is an OutputFormat to serialize {@link org.apache.flink.api.java.tuple.Tuple}s to text. The
 * output is structured by record delimiters and field delimiters as common in CSV files. Record
 * delimiter separate records from each other ('\n' is common). Field delimiters separate fields
 * within a record.
 */
@PublicEvolving
public class CsvOutputFormat<T extends Tuple> extends FileOutputFormat<T>
        implements InputTypeConfigurable {
    private static final long serialVersionUID = 1L;

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(CsvOutputFormat.class);

    // --------------------------------------------------------------------------------------------

    public static final String DEFAULT_LINE_DELIMITER = CsvInputFormat.DEFAULT_LINE_DELIMITER;

    public static final String DEFAULT_FIELD_DELIMITER =
            String.valueOf(CsvInputFormat.DEFAULT_FIELD_DELIMITER);

    // --------------------------------------------------------------------------------------------

    private transient Writer wrt;

    private String fieldDelimiter;

    private String recordDelimiter;

    private String charsetName;

    private boolean allowNullValues = true;

    private boolean quoteStrings = false;

    // --------------------------------------------------------------------------------------------
    // Constructors and getters/setters for the configurable parameters
    // --------------------------------------------------------------------------------------------

    /**
     * Creates an instance of CsvOutputFormat. Lines are separated by the newline character '\n',
     * fields are separated by ','.
     *
     * @param outputPath The path where the CSV file is written.
     */
    public CsvOutputFormat(Path outputPath) {
        this(outputPath, DEFAULT_LINE_DELIMITER, DEFAULT_FIELD_DELIMITER);
    }

    /**
     * Creates an instance of CsvOutputFormat. Lines are separated by the newline character '\n',
     * fields by the given field delimiter.
     *
     * @param outputPath The path where the CSV file is written.
     * @param fieldDelimiter The delimiter that is used to separate fields in a tuple.
     */
    public CsvOutputFormat(Path outputPath, String fieldDelimiter) {
        this(outputPath, DEFAULT_LINE_DELIMITER, fieldDelimiter);
    }

    /**
     * Creates an instance of CsvOutputFormat.
     *
     * @param outputPath The path where the CSV file is written.
     * @param recordDelimiter The delimiter that is used to separate the tuples.
     * @param fieldDelimiter The delimiter that is used to separate fields in a tuple.
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
        this.allowNullValues = false;
    }

    /**
     * Configures the format to either allow null values (writing an empty field), or to throw an
     * exception when encountering a null field.
     *
     * <p>by default, null values are disallowed.
     *
     * @param allowNulls Flag to indicate whether the output format should accept null values.
     */
    public void setAllowNullValues(boolean allowNulls) {
        this.allowNullValues = allowNulls;
    }

    /**
     * Sets the charset with which the CSV strings are written to the file. If not specified, the
     * output format uses the systems default character encoding.
     *
     * @param charsetName The name of charset to use for encoding the output.
     */
    public void setCharsetName(String charsetName) {
        this.charsetName = charsetName;
    }

    /**
     * Configures whether the output format should quote string values. String values are fields of
     * type {@link java.lang.String} and {@link org.apache.flink.types.StringValue}, as well as all
     * subclasses of the latter.
     *
     * <p>By default, strings are not quoted.
     *
     * @param quoteStrings Flag indicating whether string fields should be quoted.
     */
    public void setQuoteStrings(boolean quoteStrings) {
        this.quoteStrings = quoteStrings;
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        super.open(taskNumber, numTasks);
        this.wrt =
                this.charsetName == null
                        ? new OutputStreamWriter(new BufferedOutputStream(this.stream, 4096))
                        : new OutputStreamWriter(
                                new BufferedOutputStream(this.stream, 4096), this.charsetName);
    }

    @Override
    public void close() throws IOException {
        if (wrt != null) {
            this.wrt.flush();
            this.wrt.close();
        }
        super.close();
    }

    @Override
    public void writeRecord(T element) throws IOException {
        int numFields = element.getArity();

        for (int i = 0; i < numFields; i++) {
            Object v = element.getField(i);
            if (v != null) {
                if (i != 0) {
                    this.wrt.write(this.fieldDelimiter);
                }

                if (quoteStrings) {
                    if (v instanceof String || v instanceof StringValue) {
                        this.wrt.write('"');
                        this.wrt.write(v.toString());
                        this.wrt.write('"');
                    } else {
                        this.wrt.write(v.toString());
                    }
                } else {
                    this.wrt.write(v.toString());
                }
            } else {
                if (this.allowNullValues) {
                    if (i != 0) {
                        this.wrt.write(this.fieldDelimiter);
                    }
                } else {
                    throw new RuntimeException(
                            "Cannot write tuple with <null> value at position: " + i);
                }
            }
        }

        // add the record delimiter
        this.wrt.write(this.recordDelimiter);
    }

    // --------------------------------------------------------------------------------------------
    @Override
    public String toString() {
        return "CsvOutputFormat (path: "
                + this.getOutputFilePath()
                + ", delimiter: "
                + this.fieldDelimiter
                + ")";
    }

    /**
     * The purpose of this method is solely to check whether the data type to be processed is in
     * fact a tuple type.
     */
    @Override
    public void setInputType(TypeInformation<?> type, ExecutionConfig executionConfig) {
        if (!type.isTupleType()) {
            throw new InvalidProgramException(
                    "The "
                            + CsvOutputFormat.class.getSimpleName()
                            + " can only be used to write tuple data sets.");
        }
    }
}
