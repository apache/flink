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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableSet;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Serialization schema that serializes {@link Row} over {@link Message} into Protobuf bytes.
 *
 * <p>Influenced by {@link org.apache.flink.formats.avro.AvroRowSerializationSchema}. Enum and
 * oneof
 * is not supported yet.
 */
@PublicEvolving
public class ProtobufRowSerializationSchema implements SerializationSchema<Row> {

	private final Class<? extends Message> protoClass;
	private final RowTypeInfo rowSchema;
	private static final Logger LOG = LoggerFactory.getLogger(ProtobufRowSerializationSchema.class);
	private transient Map<String, FieldDescriptor> fieldMap; // FieldDescriptor isn't serializable

	public ProtobufRowSerializationSchema(
		Class<? extends Message> protoClass, RowTypeInfo rowSchema) {
		this.protoClass = checkNotNull(protoClass, "protoClass object is null");
		this.rowSchema = checkNotNull(rowSchema, "rowSchema object is null");
	}

	@Override
	public byte[] serialize(Row row) {
		if (fieldMap == null) {
			fieldMap = getFieldMap(ProtobufUtils.getMessageDescriptor(protoClass));
		}
		Message protoMessage = convertToMessage(protoClass, row, rowSchema, fieldMap);
		return protoMessage.toByteArray();
	}

	private static Map<String, FieldDescriptor> getFieldMap(Descriptor protoDescriptor) {
		List<FieldDescriptor> fieldDescriptors = protoDescriptor.getFields();
		Map<String, FieldDescriptor> fieldMap = new HashMap<>();
		for (FieldDescriptor fieldDescriptor : fieldDescriptors) {
			Set<String> equivalentNames =
				ImmutableSet.of(
					fieldDescriptor.getJsonName().toLowerCase() + "_",
					fieldDescriptor.getName(),
					fieldDescriptor.getJsonName());

			for (String equivalentName : equivalentNames) {
				if (fieldMap.containsKey(equivalentName)) {
					throw new RuntimeException(
						equivalentNames.toString()
							+ " are considered equivalent, but similar field exists already "
							+ fieldMap.get(equivalentName));
				} else {
					fieldMap.put(equivalentName, fieldDescriptor);
				}
			}
		}
		return fieldMap;
	}

	/**
	 * Converts a (nested) Flink {@link Row} into Protobuf's {@link Message}.
	 */
	private static Message convertToMessage(
		Class<? extends Message> protoClass,
		Row row,
		RowTypeInfo rowSchema,
		Map<String, FieldDescriptor> fieldMap) {

		// TODO: this class is heavily influenced from:
		// https://github.com/apache/flink/blob/release-1.4/flink-formats/flink-avro/src/main/java/org/apache/flink/formats/avro/AvroRowSerializationSchema.java#L118
		// where it is a recursive function and the tests seem to contain nested Row inside Row,
		// in our case we are not seeing Row inside Row, but rather other complex TypeInformation objs
		// not quite sure if it is because of how ProtoTypeInformation works, but something to be aware
		// of that we probably are not supporting certain scenarios.
		Builder protoBuilder = ProtobufUtils.getNewBuilder(protoClass);

		for (int i = 0; i < row.getArity(); i++) {
			final String fieldName = rowSchema.getFieldNames()[i].toLowerCase();
			final TypeInformation<?> fieldType = rowSchema.getFieldTypes()[i];

			if (!fieldMap.containsKey(fieldName)) {
				LOG.debug("Ignored - field: {} not found in protobuf class: {}", fieldName,
					protoClass);
				continue;
			}

			final FieldDescriptor fieldDescriptor = fieldMap.get(fieldName);

			// TODO: a type validation would be nice here to avoid runtime exception and showing
			// more verbose error message. But in its current state there should still be a type
			// cast error downstream, hopefully that should help avoid bad things from
			// happening. As of now we check all types except ENUM & LIST

			final Object fieldValue = row.getField(i);
			if (fieldDescriptor.getJavaType() == JavaType.ENUM) {
				EnumValueDescriptor enumValue =
					fieldDescriptor.getEnumType().findValueByName((String) fieldValue);
				setField(protoBuilder, fieldDescriptor, enumValue);
			} else if (fieldType instanceof ListTypeInfo) {
				ListTypeInfo listTypeInfo = (ListTypeInfo) fieldType;
				listTypeInfo.getElementTypeInfo().getTypeClass();

				if (fieldValue.getClass().isArray()) {
					List<Object> elements = Arrays.asList((Object[]) fieldValue);
					setField(protoBuilder, fieldDescriptor, elements);
				} else if (row.getField(i) instanceof List) {
					setField(protoBuilder, fieldDescriptor, fieldValue);
				} else {
					throw new IllegalStateException(
						String.format(
							"Expected Array or List for ListTypeInfo, instead found: %s, value: %s",
							row.getField(i).getClass(), row.getField(i)));
				}
			} else if (fieldType.isBasicType()) {
				JavaType expectedProtoType = ProtoFlinkMapping.FLINK_TO_PROTO_TYPES.get(fieldType);
				checkState(
					fieldDescriptor.getJavaType().equals(expectedProtoType),
					"Expected proto type: %s, found proto type: %s, for field: %s",
					expectedProtoType,
					fieldDescriptor.getJavaType(),
					fieldDescriptor.getName());

				setField(protoBuilder, fieldDescriptor, fieldValue);
			} else if (fieldType instanceof PojoTypeInfo) {
				checkState(
					fieldDescriptor.getJavaType() == JavaType.MESSAGE,
					"Expected proto type: MESSAGE, found proto type: %s, for field: %s",
					fieldDescriptor.getJavaType(),
					fieldDescriptor.getName());
				setField(protoBuilder, fieldDescriptor, fieldValue);
			}
		}

		return protoBuilder.build();
	}

	private static void setField(
		Builder protoBuilder, FieldDescriptor fieldDescriptor, Object fieldValue) {
		if (fieldValue != null) {
			protoBuilder.setField(fieldDescriptor, fieldValue);
		}
	}
}
