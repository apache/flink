/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.common.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * Types.
 */
public class Types {
	public static final TypeInformation <Void> VOID = TypeInformation.of(Void.class);
	public static final TypeInformation <String> STRING = TypeInformation.of(String.class);
	public static final TypeInformation <Byte> BYTE = TypeInformation.of(Byte.class);
	public static final TypeInformation <Boolean> BOOLEAN = TypeInformation.of(Boolean.class);
	public static final TypeInformation <Short> SHORT = TypeInformation.of(Short.class);
	public static final TypeInformation <Integer> INT = TypeInformation.of(Integer.class);
	public static final TypeInformation <Long> LONG = TypeInformation.of(Long.class);
	public static final TypeInformation <Float> FLOAT = TypeInformation.of(Float.class);
	public static final TypeInformation <Double> DOUBLE = TypeInformation.of(Double.class);
	public static final TypeInformation <Character> CHAR = TypeInformation.of(Character.class);
	public static final TypeInformation <BigDecimal> BIG_DEC = TypeInformation.of(BigDecimal.class);
	public static final TypeInformation <BigInteger> BIG_INT = TypeInformation.of(BigInteger.class);
	public static final TypeInformation <Date> SQL_DATE = TypeInformation.of(Date.class);
	public static final TypeInformation <Time> SQL_TIME = TypeInformation.of(Time.class);
	public static final TypeInformation <Timestamp> SQL_TIMESTAMP = TypeInformation.of(Timestamp.class);

	public static TypeInformation <Row> getRowType(TypeInformation <?>... types) {
		return new RowTypeInfo(types);
	}

	public static TypeInformation <Row> getRowTypeWithName(String[] fieldNames, TypeInformation <?>... types) {
		return new RowTypeInfo(types, fieldNames);
	}

}
