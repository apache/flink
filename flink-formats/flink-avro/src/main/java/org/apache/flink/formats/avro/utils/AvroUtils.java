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

package org.apache.flink.formats.avro.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;

/**
 * A collection of utilities around Avro.
 */
public final class AvroUtils {

	public static <E>DatumReader<E> createDatumReader(TypeInformation<E> type) {
		return createDatumReader(type.getTypeClass());
	}

	public static <E>DatumReader<E> createDatumReader(Class<E> type) {
		if (type == org.apache.avro.generic.GenericRecord.class) {
			return new GenericDatumReader<>();
		} else if (SpecificRecordBase.class.isAssignableFrom(type)) {
			return new SpecificDatumReader<>(type);
		} else {
			return new ReflectDatumReader<>(type);
		}
	}
}
