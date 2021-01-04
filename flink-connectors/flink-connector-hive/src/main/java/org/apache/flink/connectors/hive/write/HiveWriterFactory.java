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

package org.apache.flink.connectors.hive.write;

import org.apache.flink.connectors.hive.CachedSerializedValue;
import org.apache.flink.connectors.hive.FlinkHiveException;
import org.apache.flink.connectors.hive.JobConfWrapper;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.data.util.DataFormatConverters.DataFormatConverter;
import org.apache.flink.table.functions.hive.conversion.HiveInspectors;
import org.apache.flink.table.functions.hive.conversion.HiveObjectConversion;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;

/** Factory for creating {@link RecordWriter} and converters for writing. */
public class HiveWriterFactory implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Class hiveOutputFormatClz;

    private final CachedSerializedValue<SerDeInfo> serDeInfo;

    private final String[] allColumns;

    private final DataType[] allTypes;

    private final String[] partitionColumns;

    private final Properties tableProperties;

    private final JobConfWrapper confWrapper;

    private final HiveShim hiveShim;

    private final boolean isCompressed;

    // SerDe in Hive-1.2.1 and Hive-2.3.4 can be of different classes, make sure to use a common
    // base class
    private transient Serializer recordSerDe;

    /** Field number excluding partition fields. */
    private transient int formatFields;

    // to convert Flink object to Hive object
    private transient HiveObjectConversion[] hiveConversions;

    private transient DataFormatConverter[] converters;

    // StructObjectInspector represents the hive row structure.
    private transient StructObjectInspector formatInspector;

    private transient boolean initialized;

    public HiveWriterFactory(
            JobConf jobConf,
            Class hiveOutputFormatClz,
            SerDeInfo serDeInfo,
            TableSchema schema,
            String[] partitionColumns,
            Properties tableProperties,
            HiveShim hiveShim,
            boolean isCompressed) {
        Preconditions.checkArgument(
                HiveOutputFormat.class.isAssignableFrom(hiveOutputFormatClz),
                "The output format should be an instance of HiveOutputFormat");
        this.confWrapper = new JobConfWrapper(jobConf);
        this.hiveOutputFormatClz = hiveOutputFormatClz;
        try {
            this.serDeInfo = new CachedSerializedValue<>(serDeInfo);
        } catch (IOException e) {
            throw new FlinkHiveException("Failed to serialize SerDeInfo", e);
        }
        this.allColumns = schema.getFieldNames();
        this.allTypes = schema.getFieldDataTypes();
        this.partitionColumns = partitionColumns;
        this.tableProperties = tableProperties;
        this.hiveShim = hiveShim;
        this.isCompressed = isCompressed;
    }

    /** Create a {@link RecordWriter} from path. */
    public RecordWriter createRecordWriter(Path path) {
        try {
            checkInitialize();
            JobConf conf = new JobConf(confWrapper.conf());

            if (isCompressed) {
                String codecStr = conf.get(HiveConf.ConfVars.COMPRESSINTERMEDIATECODEC.varname);
                if (!StringUtils.isNullOrWhitespaceOnly(codecStr)) {
                    //noinspection unchecked
                    Class<? extends CompressionCodec> codec =
                            (Class<? extends CompressionCodec>)
                                    Class.forName(
                                            codecStr,
                                            true,
                                            Thread.currentThread().getContextClassLoader());
                    FileOutputFormat.setOutputCompressorClass(conf, codec);
                }
                String typeStr = conf.get(HiveConf.ConfVars.COMPRESSINTERMEDIATETYPE.varname);
                if (!StringUtils.isNullOrWhitespaceOnly(typeStr)) {
                    SequenceFile.CompressionType style =
                            SequenceFile.CompressionType.valueOf(typeStr);
                    SequenceFileOutputFormat.setOutputCompressionType(conf, style);
                }
            }

            return hiveShim.getHiveRecordWriter(
                    conf,
                    hiveOutputFormatClz,
                    recordSerDe.getSerializedClass(),
                    isCompressed,
                    tableProperties,
                    path);
        } catch (Exception e) {
            throw new FlinkHiveException(e);
        }
    }

    public JobConf getJobConf() {
        return confWrapper.conf();
    }

    private void checkInitialize() throws Exception {
        if (initialized) {
            return;
        }

        JobConf jobConf = confWrapper.conf();
        Object serdeLib =
                Class.forName(serDeInfo.deserializeValue().getSerializationLib()).newInstance();
        Preconditions.checkArgument(
                serdeLib instanceof Serializer && serdeLib instanceof Deserializer,
                "Expect a SerDe lib implementing both Serializer and Deserializer, but actually got "
                        + serdeLib.getClass().getName());
        this.recordSerDe = (Serializer) serdeLib;
        ReflectionUtils.setConf(recordSerDe, jobConf);

        // TODO: support partition properties, for now assume they're same as table properties
        SerDeUtils.initializeSerDe((Deserializer) recordSerDe, jobConf, tableProperties, null);

        this.formatFields = allColumns.length - partitionColumns.length;
        this.hiveConversions = new HiveObjectConversion[formatFields];
        this.converters = new DataFormatConverter[formatFields];
        List<ObjectInspector> objectInspectors = new ArrayList<>(hiveConversions.length);
        for (int i = 0; i < formatFields; i++) {
            DataType type = allTypes[i];
            ObjectInspector objectInspector = HiveInspectors.getObjectInspector(type);
            objectInspectors.add(objectInspector);
            hiveConversions[i] =
                    HiveInspectors.getConversion(objectInspector, type.getLogicalType(), hiveShim);
            converters[i] = DataFormatConverters.getConverterForDataType(type);
        }

        this.formatInspector =
                ObjectInspectorFactory.getStandardStructObjectInspector(
                        Arrays.asList(allColumns).subList(0, formatFields), objectInspectors);
        this.initialized = true;
    }

    public Function<Row, Writable> createRowConverter() {
        return row -> {
            List<Object> fields = new ArrayList<>(formatFields);
            for (int i = 0; i < formatFields; i++) {
                fields.add(hiveConversions[i].toHiveObject(row.getField(i)));
            }
            return serialize(fields);
        };
    }

    public Function<RowData, Writable> createRowDataConverter() {
        return row -> {
            List<Object> fields = new ArrayList<>(formatFields);
            for (int i = 0; i < formatFields; i++) {
                fields.add(hiveConversions[i].toHiveObject(converters[i].toExternal(row, i)));
            }
            return serialize(fields);
        };
    }

    private Writable serialize(List<Object> fields) {
        try {
            return recordSerDe.serialize(fields, formatInspector);
        } catch (SerDeException e) {
            throw new FlinkHiveException(e);
        }
    }
}
