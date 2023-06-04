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

package org.apache.flink.connector.datagen.table;

import org.apache.flink.annotation.Internal;

/** Utilities for {@link DataGenConnectorOptions}. */
@Internal
public class DataGenConnectorOptionsUtil {

    public static final Long ROWS_PER_SECOND_DEFAULT_VALUE = 10000L;

    public static final String FIELDS = "fields";
    public static final String KIND = "kind";
    public static final String START = "start";
    public static final String END = "end";
    public static final String MIN = "min";
    public static final String MAX = "max";
    public static final String MAX_PAST = "max-past";
    public static final String LENGTH = "length";
    public static final String NULL_RATE = "null-rate";

    public static final String SEQUENCE = "sequence";
    public static final String RANDOM = "random";

    private DataGenConnectorOptionsUtil() {}
}
