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

import org.apache.flink.connectors.hive.FlinkHiveException;
import org.apache.flink.connectors.hive.JobConfWrapper;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.hive.conversion.HiveInspectors;
import org.apache.flink.table.functions.hive.conversion.HiveObjectConversion;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.runtime.script.ScriptTransformIOInfo;
import org.apache.flink.table.runtime.util.StreamRecordCollector;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.hadoop.hive.ql.exec.RecordReader;
import org.apache.hadoop.hive.ql.exec.RecordWriter;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

/** The operator for Hive's "transform xxx using 'script'". */
public class HiveScriptTransformOperator extends TableStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData>, BoundedOneInput {

    // timeout for wait for the termination of the script process
    private static final long TIMEOUT_IN_SECONDS = 10L;

    private final int[] inputIndices;
    private final String script;
    private final ScriptTransformIOInfo scriptTransformIOInfo;
    private final LogicalType inputType;
    private final LogicalType outputType;
    private final RowData.FieldGetter[] fieldGetters;
    private final JobConfWrapper jobConfWrapper;

    private transient StreamRecordCollector<RowData> collector;
    private transient Process scriptProcess;
    private transient InputStream inputStream;
    private transient OutputStream outputStream;

    private transient AbstractSerDe outputSerDe;
    private transient RecordWriter recordWriter;
    private transient AbstractSerDe inputSerDe;
    private transient RecordReader recordReader;

    private transient HiveObjectConversion[] hiveObjectConversions;
    private transient StructObjectInspector structInputInspector;

    private transient HiveScriptTransformOutReadThread outReadThread;
    private transient CircularBuffer errorBuffer;
    private transient Thread errorReadThread;

    public HiveScriptTransformOperator(
            int[] inputIndices,
            String script,
            ScriptTransformIOInfo scriptTransformIOInfo,
            LogicalType inputType,
            LogicalType outputType) {
        this.inputIndices = inputIndices;
        this.script = script;
        this.scriptTransformIOInfo = scriptTransformIOInfo;
        this.inputType = inputType;

        List<LogicalType> fields = inputType.getChildren();
        fieldGetters =
                IntStream.range(0, fields.size())
                        .mapToObj(pos -> RowData.createFieldGetter(fields.get(pos), pos))
                        .toArray(RowData.FieldGetter[]::new);
        this.outputType = outputType;
        this.jobConfWrapper = (JobConfWrapper) scriptTransformIOInfo.getSerializableConf();
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.collector = new StreamRecordCollector<>(output);
        initScriptProc();
        // init for input of script
        initScriptInputSerDe();
        initScriptInputInfo();
        initScriptInputWriter();
        // init for output of script
        initOutputSerDe();
        initOutputReader();
        initScriptOutPutReadThread();
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        RowData rowData = element.getValue();
        List<Object> values = new ArrayList<>();
        for (int i = 0; i < inputIndices.length; i++) {
            values.add(
                    hiveObjectConversions[i].toHiveObject(
                            fieldGetters[inputIndices[i]].getFieldOrNull(rowData)));
        }
        Writable writable = inputSerDe.serialize(values, structInputInspector);
        recordWriter.write(writable);
    }

    @Override
    public void endInput() throws Exception {
        // close the script process to make the output of script available
        closeScriptProc();
    }

    @Override
    public void close() throws Exception {
        closeScriptProc();
        super.close();
    }

    private void initScriptProc() throws IOException {
        scriptProcess =
                new ScriptProcessBuilder(script, jobConfWrapper.conf(), getOperatorID()).build();
        inputStream = scriptProcess.getInputStream();
        outputStream = scriptProcess.getOutputStream();
        InputStream errorStream = scriptProcess.getErrorStream();

        // we need a thread to read the error outputs, otherwise, the error will chock up the
        // buffer and the input logic will be hanged.
        // we redirect it to a limited buffer to avoid too many error outputs
        errorBuffer = new CircularBuffer();
        errorReadThread =
                new RedirectStreamThread(
                        errorStream,
                        errorBuffer,
                        String.format("Thread-%s-error-consumer", getClass().getSimpleName()));
        errorReadThread.start();
    }

    private void closeScriptProc() {
        if (recordWriter != null) {
            try {
                recordWriter.close();
            } catch (IOException e) {
                LOG.warn("Exception in closing RecordWriter.", e);
            }
            recordWriter = null;
        }
        if (scriptProcess != null) {
            try {
                boolean exitRes = scriptProcess.waitFor(TIMEOUT_IN_SECONDS, TimeUnit.SECONDS);
                if (!exitRes) {
                    LOG.warn(
                            "Transformation script process exits timeout in {} seconds",
                            TIMEOUT_IN_SECONDS);
                }
            } catch (InterruptedException e) {
                LOG.warn("Exception in waiting for transformation script process.", e);
            }
            if (scriptProcess.isAlive()) {
                scriptProcess.destroy();
            } else {
                int exitVal = scriptProcess.exitValue();
                if (exitVal != 0) {
                    throw new FlinkHiveException(
                            String.format(
                                    "Script failed with code %d, message %s.",
                                    exitVal, errorBuffer.toString()));
                }
            }
            scriptProcess = null;
        }

        if (outReadThread != null) {
            try {
                outReadThread.join();
            } catch (InterruptedException e) {
                LOG.warn("Exception in closing output read thread.", e);
            }
            outReadThread = null;
        }

        if (errorReadThread != null) {
            try {
                errorReadThread.join();
            } catch (InterruptedException e) {
                LOG.warn("Exception in closing error read thread.", e);
            }
            errorReadThread = null;
        }
    }

    private AbstractSerDe initSerDe(String serdeClassName, Map<String, String> props)
            throws Exception {
        Class<?> serdeClz = Class.forName(serdeClassName);
        AbstractSerDe abstractSerDe = (AbstractSerDe) serdeClz.newInstance();
        Properties properties = new Properties();
        properties.putAll(props);
        abstractSerDe.initialize(
                ((JobConfWrapper) scriptTransformIOInfo.getSerializableConf()).conf(), properties);
        return abstractSerDe;
    }

    private void initScriptInputSerDe() throws Exception {
        inputSerDe =
                initSerDe(
                        scriptTransformIOInfo.getInputSerdeClass(),
                        scriptTransformIOInfo.getInputSerdeProps());
    }

    /** Initialize the Hive object conversion and inspector for script's input. */
    private void initScriptInputInfo() {
        StructObjectInspector inputObjectInspector =
                (StructObjectInspector) HiveInspectors.getObjectInspector(inputType);

        // get the object inspector of projected input
        ObjectInspector[] projectInputObjectInspector = new ObjectInspector[inputIndices.length];
        ObjectInspector[] inputObjectInspectors =
                inputObjectInspector.getAllStructFieldRefs().stream()
                        .map(StructField::getFieldObjectInspector)
                        .toArray(ObjectInspector[]::new);

        // create conversions to convert Flink's object to Hive's object
        hiveObjectConversions = new HiveObjectConversion[inputIndices.length];
        for (int i = 0; i < inputIndices.length; i++) {
            int inputIndex = inputIndices[i];
            projectInputObjectInspector[i] = inputObjectInspectors[inputIndex];
            hiveObjectConversions[i] =
                    HiveInspectors.getConversion(
                            projectInputObjectInspector[i],
                            inputType.getChildren().get(inputIndex),
                            HiveShimLoader.loadHiveShim(HiveShimLoader.getHiveVersion()));
        }

        String[] inputCols =
                scriptTransformIOInfo
                        .getInputSerdeProps()
                        .get(serdeConstants.LIST_COLUMNS)
                        .split(",");
        // combine the list of object inspectors for projected input to a struct object inspector
        structInputInspector =
                ObjectInspectorFactory.getStandardStructObjectInspector(
                        Arrays.asList(inputCols), Arrays.asList(projectInputObjectInspector));
    }

    /** Initialize the writer for writing to the script transform process . */
    private void initScriptInputWriter() throws Exception {
        Class<?> recordWriterCls = Class.forName(scriptTransformIOInfo.getRecordWriterClass());
        recordWriter = (RecordWriter) recordWriterCls.newInstance();
        recordWriter.initialize(
                outputStream,
                ((JobConfWrapper) scriptTransformIOInfo.getSerializableConf()).conf());
    }

    private void initOutputSerDe() throws Exception {
        outputSerDe =
                initSerDe(
                        scriptTransformIOInfo.getOutputSerdeClass(),
                        scriptTransformIOInfo.getOutputSerdeProps());
    }

    /** Initialize the reader for reading output of the script transform process. */
    private void initOutputReader() throws Exception {
        Class<?> readerCls = Class.forName(scriptTransformIOInfo.getRecordReaderClass());
        recordReader = (RecordReader) readerCls.newInstance();
        Properties properties = new Properties();
        properties.putAll(scriptTransformIOInfo.getOutputSerdeProps());
        recordReader.initialize(
                inputStream,
                ((JobConfWrapper) scriptTransformIOInfo.getSerializableConf()).conf(),
                properties);
    }

    /** Start a thread to read output from script process and collect to upstream. */
    private void initScriptOutPutReadThread() throws Exception {
        StructObjectInspector outputStructObjectInspector =
                (StructObjectInspector) outputSerDe.getObjectInspector();
        outReadThread =
                new HiveScriptTransformOutReadThread(
                        recordReader,
                        outputType,
                        outputSerDe,
                        outputStructObjectInspector,
                        collector);
        outReadThread.start();
    }
}
