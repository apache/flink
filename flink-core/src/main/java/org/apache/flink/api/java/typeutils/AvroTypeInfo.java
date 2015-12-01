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


package org.apache.flink.api.java.typeutils;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

/**
 * Special type information to generate a special AvroTypeInfo for Avro POJOs (implementing SpecificRecordBase, the typed Avro POJOs)
 *
 * Proceeding: It uses a regular pojo type analysis and replaces all {@code GenericType<CharSequence>}
 *     with a {@code GenericType<avro.Utf8>}.
 * All other types used by Avro are standard Java types.
 * Only strings are represented as CharSequence fields and represented as Utf8 classes at runtime.
 * CharSequence is not comparable. To make them nicely usable with field expressions, we replace them here
 * by generic type infos containing Utf8 classes (which are comparable),
 *
 * This class is checked by the AvroPojoTest.
 * @param <T>
 */
@Public
public class AvroTypeInfo<T extends SpecificRecordBase> extends PojoTypeInfo<T> {
	@Experimental
	public AvroTypeInfo(Class<T> typeClass) {
		super(typeClass, generateFieldsFromAvroSchema(typeClass));
	}

	private static <T extends SpecificRecordBase> List<PojoField> generateFieldsFromAvroSchema(Class<T> typeClass) {
		PojoTypeExtractor pte = new PojoTypeExtractor();
		TypeInformation ti = pte.analyzePojo(typeClass, new ArrayList<Type>(), null, null, null);

		if(!(ti instanceof PojoTypeInfo)) {
			throw new IllegalStateException("Expecting type to be a PojoTypeInfo");
		}
		PojoTypeInfo pti =  (PojoTypeInfo) ti;
		List<PojoField> newFields = new ArrayList<PojoField>(pti.getTotalFields());

		for(int i = 0; i < pti.getArity(); i++) {
			PojoField f = pti.getPojoFieldAt(i);
			TypeInformation newType = f.getTypeInformation();
			// check if type is a CharSequence
			if(newType instanceof GenericTypeInfo) {
				if((newType).getTypeClass().equals(CharSequence.class)) {
					// replace the type by a org.apache.avro.util.Utf8
					newType = new GenericTypeInfo(org.apache.avro.util.Utf8.class);
				}
			}
			PojoField newField = new PojoField(f.getField(), newType);
			newFields.add(newField);
		}
		return newFields;
	}

	private static class PojoTypeExtractor extends TypeExtractor {
		private PojoTypeExtractor() {
			super();
		}
	}
}
