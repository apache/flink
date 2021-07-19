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

package org.apache.flink.formats.protobuf;

/** Keeps protobuf constants separately. */
public class PbConstant {
    public static final String PB_METHOD_GET_DESCRIPTOR = "getDescriptor";
    public static final String PB_METHOD_PARSE_FROM = "parseFrom";
    public static final String GENERATED_DECODE_METHOD = "decode";
    public static final String GENERATED_ENCODE_METHOD = "encode";
    public static final String PB_MAP_KEY_NAME = "key";
    public static final String PB_MAP_VALUE_NAME = "value";
    public static final String PB_OUTER_CLASS_SUFFIX = "OuterClass";
}
