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

package org.apache.flink.model.triton;

/**
 * Enumeration of data types supported by Triton Inference Server.
 *
 * <p>These data types correspond to the types defined in the Triton Inference Server protocol.
 * Reference:
 * https://github.com/triton-inference-server/server/blob/main/docs/protocol/extension_model_configuration.md
 */
public enum TritonDataType {
    /** Boolean type. */
    BOOL("BOOL"),

    /** 8-bit unsigned integer. */
    UINT8("UINT8"),

    /** 16-bit unsigned integer. */
    UINT16("UINT16"),

    /** 32-bit unsigned integer. */
    UINT32("UINT32"),

    /** 64-bit unsigned integer. */
    UINT64("UINT64"),

    /** 8-bit signed integer. */
    INT8("INT8"),

    /** 16-bit signed integer. */
    INT16("INT16"),

    /** 32-bit signed integer. */
    INT32("INT32"),

    /** 64-bit signed integer. */
    INT64("INT64"),

    /** 16-bit floating point (half precision). */
    FP16("FP16"),

    /** 32-bit floating point (single precision). */
    FP32("FP32"),

    /** 64-bit floating point (double precision). */
    FP64("FP64"),

    /** String/text data. */
    BYTES("BYTES");

    private final String tritonName;

    TritonDataType(String tritonName) {
        this.tritonName = tritonName;
    }

    /** Returns the Triton protocol name for this data type. */
    public String getTritonName() {
        return tritonName;
    }

    /** Gets a TritonDataType from its Triton protocol name. */
    public static TritonDataType fromTritonName(String tritonName) {
        for (TritonDataType type : values()) {
            if (type.tritonName.equals(tritonName)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown Triton data type: " + tritonName);
    }
}
