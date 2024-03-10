/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.protobuf.registry.confluent.utils;

/**
 * Common constants used for converting Protobuf schema. Those are Confluent Schema Registry
 * specific.
 */
class CommonConstants {

    static final String CONNECT_TYPE_PROP = "connect.type";
    static final String CONNECT_TYPE_INT8 = "int8";
    static final String CONNECT_TYPE_INT16 = "int16";
    static final String PROTOBUF_DECIMAL_TYPE = "confluent.type.Decimal";
    static final String PROTOBUF_DECIMAL_LOCATION = "confluent/type/decimal.proto";
    static final String PROTOBUF_DATE_TYPE = "google.type.Date";
    static final String PROTOBUF_DATE_LOCATION = "google/type/date.proto";
    static final String PROTOBUF_TIME_TYPE = "google.type.TimeOfDay";
    static final String PROTOBUF_TIME_LOCATION = "google/type/timeofday.proto";
    static final String PROTOBUF_PRECISION_PROP = "precision";
    static final String PROTOBUF_SCALE_PROP = "scale";
    static final String PROTOBUF_TIMESTAMP_TYPE = "google.protobuf.Timestamp";
    static final String PROTOBUF_TIMESTAMP_LOCATION = "google/protobuf/timestamp.proto";
    static final String MAP_ENTRY_SUFFIX = "Entry"; // Suffix used by protoc
    static final String KEY_FIELD = "key";
    static final String VALUE_FIELD = "value";

    static final String PROTOBUF_DOUBLE_WRAPPER_TYPE = "google.protobuf.DoubleValue";
    static final String PROTOBUF_FLOAT_WRAPPER_TYPE = "google.protobuf.FloatValue";
    static final String PROTOBUF_INT64_WRAPPER_TYPE = "google.protobuf.Int64Value";
    static final String PROTOBUF_UINT64_WRAPPER_TYPE = "google.protobuf.UInt64Value";
    static final String PROTOBUF_INT32_WRAPPER_TYPE = "google.protobuf.Int32Value";
    static final String PROTOBUF_UINT32_WRAPPER_TYPE = "google.protobuf.UInt32Value";
    static final String PROTOBUF_BOOL_WRAPPER_TYPE = "google.protobuf.BoolValue";
    static final String PROTOBUF_STRING_WRAPPER_TYPE = "google.protobuf.StringValue";
    static final String PROTOBUF_BYTES_WRAPPER_TYPE = "google.protobuf.BytesValue";

    private CommonConstants() {}
}
