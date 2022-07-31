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

package org.apache.flink.table.runtime.operators.hive.script;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.functions.hive.conversion.HiveInspectors;
import org.apache.flink.table.runtime.util.StreamRecordCollector;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.hadoop.hive.ql.exec.RecordReader;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/** A read thread to read output of script transform and then collect it to downstream. */
public class HiveScriptTransformOutReadThread extends Thread {

    private static final Logger LOG =
            LoggerFactory.getLogger(HiveScriptTransformOutReadThread.class);

    private final StreamRecordCollector<RowData> collector;
    private final AbstractSerDe outSerDe;
    private final RecordReader recordReader;
    private final DataFormatConverters.DataFormatConverter[] converters;
    private final HiveShim hiveShim;
    private final StructObjectInspector outputStructObjectInspector;
    private final List<? extends StructField> structFields;
    private final Writable reusedWritableObject;

    public HiveScriptTransformOutReadThread(
            RecordReader recordReader,
            LogicalType outputType,
            AbstractSerDe outSerDe,
            StructObjectInspector structObjectInspector,
            Writable reusedWritableObject,
            StreamRecordCollector<RowData> collector) {
        this.recordReader = recordReader;
        this.outSerDe = outSerDe;
        converters = new DataFormatConverters.DataFormatConverter[outputType.getChildren().size()];
        for (int i = 0; i < converters.length; i++) {
            converters[i] =
                    DataFormatConverters.getConverterForDataType(
                            DataTypes.of(outputType.getChildren().get(i)));
        }
        this.hiveShim = HiveShimLoader.loadHiveShim(HiveShimLoader.getHiveVersion());
        this.outputStructObjectInspector = structObjectInspector;
        this.reusedWritableObject = reusedWritableObject;
        this.structFields = outputStructObjectInspector.getAllStructFieldRefs();
        this.collector = collector;
        setDaemon(true);
        setName("Thread-HiveScriptTransformOutReadThread");
    }

    @Override
    public void run() {
        try {
            while (true) {
                long bytes = recordReader.next(reusedWritableObject);
                if (bytes <= 0) {
                    break;
                }
                processLine(reusedWritableObject);
            }
            LOG.info("HiveScriptTransformOutReadThread done.");
        } catch (Throwable th) {
            LOG.warn("Exception in HiveScriptTransformOutReadThread.run()", th);
        } finally {
            try {
                if (recordReader != null) {
                    recordReader.close();
                }
            } catch (Exception e) {
                LOG.warn("Error in closing RecordReader", e);
            }
        }
    }

    public void processLine(Writable line) {
        Object rawOutput;
        try {
            rawOutput = outSerDe.deserialize(line);
        } catch (SerDeException serDeException) {
            LOG.warn(
                    "Encounter SerDeException: {} when try to deserialize line: {}",
                    serDeException,
                    line);
            return;
        }
        GenericRowData rowData = new GenericRowData(converters.length);
        for (int i = 0; i < converters.length; i++) {
            Object value =
                    HiveInspectors.toFlinkObject(
                            structFields.get(i).getFieldObjectInspector(),
                            outputStructObjectInspector.getStructFieldData(
                                    rawOutput, structFields.get(i)),
                            hiveShim);
            rowData.setField(i, converters[i].toInternal(value));
        }
        collector.collect(rowData);
    }
}
