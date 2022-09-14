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

package org.apache.flink.table.runtime.script;

import java.io.Serializable;
import java.util.Map;

/**
 * The wrapper class of the input/out schema for script transform.
 *
 * <p>The data will be serialized, and then feed into the script. So, we need to how to serialize
 * the data including the serializing class {@link #inputSerdeClass} and the corresponding
 * properties {@link #inputSerdeProps} needed while doing serializing. The {@link
 * #recordWriterClass} is for how to write the serialized data.
 *
 * <p>And the output of the script need to be deserialized. So, we need to know how to deserialize
 * it, including the deserializing class {@link #outputSerdeClass} and the corresponding properties
 * {@link #outputSerdeProps}. The {@link #recordReaderClass} is for how to read the deserialized
 * data.
 *
 * <p>{@link #serializableConf} is the configuration that the writer/read may need.
 */
public class ScriptTransformIOInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String inputSerdeClass;
    private final Map<String, String> inputSerdeProps;
    private final String recordWriterClass;

    private final String outputSerdeClass;
    private final Map<String, String> outputSerdeProps;
    private final String recordReaderClass;

    private final Serializable serializableConf;

    public ScriptTransformIOInfo(
            String inputSerdeClass,
            Map<String, String> inputSerdeProps,
            String outputSerdeClass,
            Map<String, String> outputSerdeProps,
            String recordWriterClass,
            String recordReaderClass,
            Serializable serializableConf) {
        this.inputSerdeProps = inputSerdeProps;
        this.outputSerdeProps = outputSerdeProps;
        this.inputSerdeClass = inputSerdeClass;
        this.outputSerdeClass = outputSerdeClass;
        this.recordReaderClass = recordReaderClass;
        this.recordWriterClass = recordWriterClass;
        this.serializableConf = serializableConf;
    }

    public Map<String, String> getInputSerdeProps() {
        return inputSerdeProps;
    }

    public Map<String, String> getOutputSerdeProps() {
        return outputSerdeProps;
    }

    public String getInputSerdeClass() {
        return inputSerdeClass;
    }

    public String getOutputSerdeClass() {
        return outputSerdeClass;
    }

    public String getRecordReaderClass() {
        return recordReaderClass;
    }

    public String getRecordWriterClass() {
        return recordWriterClass;
    }

    public Serializable getSerializableConf() {
        return serializableConf;
    }

    @Override
    public String toString() {
        return "ScriptTransformIOInfo{"
                + "inputSerdeProps="
                + inputSerdeProps
                + ", outputSerdeProps="
                + outputSerdeProps
                + ", inputSerdeClass='"
                + inputSerdeClass
                + '\''
                + ", outputSerdeClass='"
                + outputSerdeClass
                + '\''
                + ", recordReaderClass='"
                + recordReaderClass
                + '\''
                + ", recordWriterClass='"
                + recordWriterClass
                + '\''
                + '}';
    }
}
