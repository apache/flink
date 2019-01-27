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

package org.apache.flink.table.api.types;

import java.util.Arrays;
import java.util.List;

/**
 * This class enumerates all supported types of the Flink SQL/Table API.
 */
public class Types {

	public static final StringType STRING = StringType.INSTANCE;

	public static final BooleanType BOOLEAN = BooleanType.INSTANCE;

	public static final DoubleType DOUBLE = DoubleType.INSTANCE;

	public static final FloatType FLOAT = FloatType.INSTANCE;

	public static final ByteType BYTE = ByteType.INSTANCE;

	public static final IntType INT = IntType.INSTANCE;

	public static final LongType LONG = LongType.INSTANCE;

	public static final ShortType SHORT = ShortType.INSTANCE;

	public static final CharType CHAR = CharType.INSTANCE;

	public static final ByteArrayType BYTE_ARRAY = ByteArrayType.INSTANCE;

	public static final DateType DATE = DateType.DATE;

	public static final TimestampType TIMESTAMP = TimestampType.TIMESTAMP;

	public static final TimeType TIME = TimeType.INSTANCE;

	public static final DateType INTERVAL_MONTHS = DateType.INTERVAL_MONTHS;

	public static final TimestampType INTERVAL_MILLIS = TimestampType.INTERVAL_MILLIS;

	public static final TimestampType ROWTIME_INDICATOR = TimestampType.ROWTIME_INDICATOR;

	public static final TimestampType PROCTIME_INDICATOR = TimestampType.PROCTIME_INDICATOR;

	public static final IntervalRowsType INTERVAL_ROWS = IntervalRowsType.INSTANCE;

	public static final IntervalRangeType INTERVAL_RANGE = IntervalRangeType.INSTANCE;

	public static final List<PrimitiveType> INTEGRAL_TYPES = Arrays.asList(BYTE, SHORT, INT, LONG);

	public static final List<PrimitiveType> FRACTIONAL_TYPES = Arrays.asList(FLOAT, DOUBLE);

	public static final DecimalType DECIMAL = DecimalType.SYSTEM_DEFAULT;
}
