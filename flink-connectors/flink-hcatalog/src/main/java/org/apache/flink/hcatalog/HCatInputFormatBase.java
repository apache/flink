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

package org.apache.flink.hcatalog;

import org.apache.flink.api.common.io.LocatableInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.hadoop.mapreduce.utils.HadoopUtils;
import org.apache.flink.api.java.hadoop.mapreduce.wrapper.HadoopInputSplit;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.WritableTypeInfo;
import org.apache.flink.core.io.InputSplitAssigner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A InputFormat to read from HCatalog tables. The InputFormat supports projection (selection and
 * order of fields) and partition filters.
 *
 * <p>Data can be returned as {@link org.apache.hive.hcatalog.data.HCatRecord} or Flink-native
 * tuple.
 *
 * <p>Note: Flink tuples might only support a limited number of fields (depending on the API).
 *
 * @param <T>
 */
public abstract class HCatInputFormatBase<T> extends RichInputFormat<T, HadoopInputSplit>
        implements ResultTypeQueryable<T> {

    private static final long serialVersionUID = 1L;

    private Configuration configuration;

    private org.apache.hive.hcatalog.mapreduce.HCatInputFormat hCatInputFormat;
    private RecordReader<WritableComparable, HCatRecord> recordReader;
    private boolean fetched = false;
    private boolean hasNext;

    protected String[] fieldNames = new String[0];
    protected HCatSchema outputSchema;

    private TypeInformation<T> resultType;

    public HCatInputFormatBase() {}

    /**
     * Creates a HCatInputFormat for the given database and table. By default, the InputFormat
     * returns {@link org.apache.hive.hcatalog.data.HCatRecord}. The return type of the InputFormat
     * can be changed to Flink-native tuples by calling {@link HCatInputFormatBase#asFlinkTuples()}.
     *
     * @param database The name of the database to read from.
     * @param table The name of the table to read.
     * @throws java.io.IOException
     */
    public HCatInputFormatBase(String database, String table) throws IOException {
        this(database, table, new Configuration());
    }

    /**
     * Creates a HCatInputFormat for the given database, table, and {@link
     * org.apache.hadoop.conf.Configuration}. By default, the InputFormat returns {@link
     * org.apache.hive.hcatalog.data.HCatRecord}. The return type of the InputFormat can be changed
     * to Flink-native tuples by calling {@link HCatInputFormatBase#asFlinkTuples()}.
     *
     * @param database The name of the database to read from.
     * @param table The name of the table to read.
     * @param config The Configuration for the InputFormat.
     * @throws java.io.IOException
     */
    public HCatInputFormatBase(String database, String table, Configuration config)
            throws IOException {
        super();
        this.configuration = config;
        HadoopUtils.mergeHadoopConf(this.configuration);

        this.hCatInputFormat =
                org.apache.hive.hcatalog.mapreduce.HCatInputFormat.setInput(
                        this.configuration, database, table);
        this.outputSchema =
                org.apache.hive.hcatalog.mapreduce.HCatInputFormat.getTableSchema(
                        this.configuration);

        // configure output schema of HCatFormat
        configuration.set("mapreduce.lib.hcat.output.schema", HCatUtil.serialize(outputSchema));
        // set type information
        this.resultType = new WritableTypeInfo(DefaultHCatRecord.class);
    }

    /**
     * Specifies the fields which are returned by the InputFormat and their order.
     *
     * @param fields The fields and their order which are returned by the InputFormat.
     * @return This InputFormat with specified return fields.
     * @throws java.io.IOException
     */
    public HCatInputFormatBase<T> getFields(String... fields) throws IOException {

        // build output schema
        ArrayList<HCatFieldSchema> fieldSchemas = new ArrayList<HCatFieldSchema>(fields.length);
        for (String field : fields) {
            fieldSchemas.add(this.outputSchema.get(field));
        }
        this.outputSchema = new HCatSchema(fieldSchemas);

        // update output schema configuration
        configuration.set("mapreduce.lib.hcat.output.schema", HCatUtil.serialize(outputSchema));

        return this;
    }

    /**
     * Specifies a SQL-like filter condition on the table's partition columns. Filter conditions on
     * non-partition columns are invalid. A partition filter can significantly reduce the amount of
     * data to be read.
     *
     * @param filter A SQL-like filter condition on the table's partition columns.
     * @return This InputFormat with specified partition filter.
     * @throws java.io.IOException
     */
    public HCatInputFormatBase<T> withFilter(String filter) throws IOException {

        // set filter
        this.hCatInputFormat.setFilter(filter);

        return this;
    }

    /**
     * Specifies that the InputFormat returns Flink tuples instead of {@link
     * org.apache.hive.hcatalog.data.HCatRecord}.
     *
     * <p>Note: Flink tuples might only support a limited number of fields (depending on the API).
     *
     * @return This InputFormat.
     * @throws org.apache.hive.hcatalog.common.HCatException
     */
    public HCatInputFormatBase<T> asFlinkTuples() throws HCatException {

        // build type information
        int numFields = outputSchema.getFields().size();
        if (numFields > this.getMaxFlinkTupleSize()) {
            throw new IllegalArgumentException(
                    "Only up to "
                            + this.getMaxFlinkTupleSize()
                            + " fields can be returned as Flink tuples.");
        }

        TypeInformation[] fieldTypes = new TypeInformation[numFields];
        fieldNames = new String[numFields];
        for (String fieldName : outputSchema.getFieldNames()) {
            HCatFieldSchema field = outputSchema.get(fieldName);

            int fieldPos = outputSchema.getPosition(fieldName);
            TypeInformation fieldType = getFieldType(field);

            fieldTypes[fieldPos] = fieldType;
            fieldNames[fieldPos] = fieldName;
        }
        this.resultType = new TupleTypeInfo(fieldTypes);

        return this;
    }

    protected abstract int getMaxFlinkTupleSize();

    private TypeInformation getFieldType(HCatFieldSchema fieldSchema) {

        switch (fieldSchema.getType()) {
            case INT:
                return BasicTypeInfo.INT_TYPE_INFO;
            case TINYINT:
                return BasicTypeInfo.BYTE_TYPE_INFO;
            case SMALLINT:
                return BasicTypeInfo.SHORT_TYPE_INFO;
            case BIGINT:
                return BasicTypeInfo.LONG_TYPE_INFO;
            case BOOLEAN:
                return BasicTypeInfo.BOOLEAN_TYPE_INFO;
            case FLOAT:
                return BasicTypeInfo.FLOAT_TYPE_INFO;
            case DOUBLE:
                return BasicTypeInfo.DOUBLE_TYPE_INFO;
            case STRING:
                return BasicTypeInfo.STRING_TYPE_INFO;
            case BINARY:
                return PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;
            case ARRAY:
                return new GenericTypeInfo(List.class);
            case MAP:
                return new GenericTypeInfo(Map.class);
            case STRUCT:
                return new GenericTypeInfo(List.class);
            default:
                throw new IllegalArgumentException(
                        "Unknown data type \"" + fieldSchema.getType() + "\" encountered.");
        }
    }

    /**
     * Returns the {@link org.apache.hadoop.conf.Configuration} of the HCatInputFormat.
     *
     * @return The Configuration of the HCatInputFormat.
     */
    public Configuration getConfiguration() {
        return this.configuration;
    }

    /**
     * Returns the {@link org.apache.hive.hcatalog.data.schema.HCatSchema} of the {@link
     * org.apache.hive.hcatalog.data.HCatRecord} returned by this InputFormat.
     *
     * @return The HCatSchema of the HCatRecords returned by this InputFormat.
     */
    public HCatSchema getOutputSchema() {
        return this.outputSchema;
    }

    // --------------------------------------------------------------------------------------------
    //  InputFormat
    // --------------------------------------------------------------------------------------------

    @Override
    public void configure(org.apache.flink.configuration.Configuration parameters) {
        // nothing to do
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStats) throws IOException {
        // no statistics provided at the moment
        return null;
    }

    @Override
    public HadoopInputSplit[] createInputSplits(int minNumSplits) throws IOException {
        configuration.setInt("mapreduce.input.fileinputformat.split.minsize", minNumSplits);

        JobContext jobContext = new JobContextImpl(configuration, new JobID());

        List<InputSplit> splits;
        try {
            splits = this.hCatInputFormat.getSplits(jobContext);
        } catch (InterruptedException e) {
            throw new IOException("Could not get Splits.", e);
        }
        HadoopInputSplit[] hadoopInputSplits = new HadoopInputSplit[splits.size()];

        for (int i = 0; i < hadoopInputSplits.length; i++) {
            hadoopInputSplits[i] = new HadoopInputSplit(i, splits.get(i), jobContext);
        }
        return hadoopInputSplits;
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(HadoopInputSplit[] inputSplits) {
        return new LocatableInputSplitAssigner(inputSplits);
    }

    @Override
    public void open(HadoopInputSplit split) throws IOException {
        TaskAttemptContext context = new TaskAttemptContextImpl(configuration, new TaskAttemptID());

        try {
            this.recordReader =
                    this.hCatInputFormat.createRecordReader(split.getHadoopInputSplit(), context);
            this.recordReader.initialize(split.getHadoopInputSplit(), context);
        } catch (InterruptedException e) {
            throw new IOException("Could not create RecordReader.", e);
        } finally {
            this.fetched = false;
        }
    }

    @Override
    public boolean reachedEnd() throws IOException {
        if (!this.fetched) {
            fetchNext();
        }
        return !this.hasNext;
    }

    private void fetchNext() throws IOException {
        try {
            this.hasNext = this.recordReader.nextKeyValue();
        } catch (InterruptedException e) {
            throw new IOException("Could not fetch next KeyValue pair.", e);
        } finally {
            this.fetched = true;
        }
    }

    @Override
    public T nextRecord(T record) throws IOException {
        if (!this.fetched) {
            // first record
            fetchNext();
        }
        if (!this.hasNext) {
            return null;
        }
        try {

            // get next HCatRecord
            HCatRecord v = this.recordReader.getCurrentValue();
            this.fetched = false;

            if (this.fieldNames.length > 0) {
                // return as Flink tuple
                return this.buildFlinkTuple(record, v);

            } else {
                // return as HCatRecord
                return (T) v;
            }

        } catch (InterruptedException e) {
            throw new IOException("Could not get next record.", e);
        }
    }

    protected abstract T buildFlinkTuple(T t, HCatRecord record) throws HCatException;

    @Override
    public void close() throws IOException {
        this.recordReader.close();
    }

    // --------------------------------------------------------------------------------------------
    //  Custom de/serialization methods
    // --------------------------------------------------------------------------------------------

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.writeInt(this.fieldNames.length);
        for (String fieldName : this.fieldNames) {
            out.writeUTF(fieldName);
        }
        this.configuration.write(out);
    }

    @SuppressWarnings("unchecked")
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        this.fieldNames = new String[in.readInt()];
        for (int i = 0; i < this.fieldNames.length; i++) {
            this.fieldNames[i] = in.readUTF();
        }

        Configuration configuration = new Configuration();
        configuration.readFields(in);

        if (this.configuration == null) {
            this.configuration = configuration;
        }

        this.hCatInputFormat = new org.apache.hive.hcatalog.mapreduce.HCatInputFormat();
        this.outputSchema =
                (HCatSchema)
                        HCatUtil.deserialize(
                                this.configuration.get("mapreduce.lib.hcat.output.schema"));
    }

    // --------------------------------------------------------------------------------------------
    //  Result type business
    // --------------------------------------------------------------------------------------------

    @Override
    public TypeInformation<T> getProducedType() {
        return this.resultType;
    }
}
