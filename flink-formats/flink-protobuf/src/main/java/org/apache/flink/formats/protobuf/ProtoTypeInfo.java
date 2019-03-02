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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.List;

import static com.google.protobuf.Descriptors.FieldDescriptor.Type.MESSAGE;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Provides flink understood TypeInfo for protobuf classes.
 *
 * <pre>
 * Current limitation:
 *  - protobuf field names are not very nice, ends with underscore. Since flink uses reflection
 *    Field object to read the field, it cannot be modified in flight and would need change in flink
 *    core.
 *  - enum field needs to be tested
 * </pre>
 */
public class ProtoTypeInfo<T extends Message> extends PojoTypeInfo<T> {

	private final Class<Message> protoClass;
	private static final Logger LOG = LoggerFactory.getLogger(ProtoTypeInfo.class);

	public ProtoTypeInfo(Class<T> protoClass) {
		super(protoClass, getFields(protoClass));
		this.protoClass = (Class<Message>) protoClass;
	}

	/**
	 * Get PojoFields given a protobuf class.
	 *
	 * <pre>
	 *   - This needs to be static to be accessible from the constructor class
	 *   - T doesn't seem to be accessible from the static context hence uses class&lt;?&gt;?
	 * </pre>
	 */
	private static List<PojoField> getFields(Class<?> clazz) {
		checkArgument(
			Message.class.isAssignableFrom(clazz), "Expected a protobuf class but found " + clazz);
		Class<Message> protoClass = (Class<Message>) clazz;
		List<PojoField> fieldDictionary = new ArrayList<>();

		Descriptor descriptor = ProtobufUtils.getMessageDescriptor(protoClass);
		checkNotNull(descriptor, "Couldn't get descriptor from protobuf class " + protoClass);

		buildFieldDictionary(descriptor, fieldDictionary, protoClass);
		return fieldDictionary;
	}

	private static void buildFieldDictionary(
		Descriptor descriptor, List<PojoField> allFields, Class<Message> protoClass)
		throws IllegalArgumentException {
		for (FieldDescriptor fieldDescriptor : descriptor.getFields()) {
			if (!isSupportedField(fieldDescriptor)) {
				LOG.warn(
					"Skipping unsupported field named '{}'; javaType: {};",
					fieldDescriptor.getName(),
					fieldDescriptor.getJavaType());
				continue;
			}
			final Field field = ProtobufUtils.getField(protoClass, fieldDescriptor);
			final TypeInformation<?> typeInformation = getTypeInformation(field, fieldDescriptor);
			allFields.add(new PojoField(field, typeInformation));
		}
	}

	private static boolean isSupportedField(FieldDescriptor fieldDescriptor) {
		return fieldDescriptor.getContainingOneof() == null
			&& !fieldDescriptor.getJavaType().equals(JavaType.ENUM);
	}

	private static String msgForUnhandledType(FieldDescriptor fieldDescriptor) {
		return String.format(
			"Unknown/unhandled type '%s' for field '%s'",
			fieldDescriptor.getJavaType(), fieldDescriptor.getFullName());
	}

	private static TypeInformation<?> getTypeInformation(
		Field field, FieldDescriptor fieldDescriptor) {
		if (fieldDescriptor.isRepeated()) {
			TypeInformation elementType;
			if (ProtoFlinkMapping.PROTO_TO_FLINK_TYPES.containsKey(fieldDescriptor.getJavaType())) {
				elementType = ProtoFlinkMapping.PROTO_TO_FLINK_TYPES
					.get(fieldDescriptor.getJavaType());
			} else {
				checkState(
					field.getGenericType() instanceof ParameterizedType,
					msgForUnhandledType(fieldDescriptor));
				elementType =
					TypeInformation.of(
						(Class) ((ParameterizedType) field.getGenericType())
							.getActualTypeArguments()[0]);
			}
			return new ListTypeInfo(elementType);
		} else if (fieldDescriptor.getType().equals(MESSAGE)) {
			return new ProtoTypeInfo(field.getType());
		} else {
			checkState(
				ProtoFlinkMapping.PROTO_TO_FLINK_TYPES.containsKey(fieldDescriptor.getJavaType()),
				msgForUnhandledType(fieldDescriptor));
			return ProtoFlinkMapping.PROTO_TO_FLINK_TYPES.get(fieldDescriptor.getJavaType());
		}
	}

	@Override
	public TypeSerializer<T> createSerializer(ExecutionConfig config) {
		// Flink's Pojo serializer doesn't work due to two reasons:
		//  - protobuf classes not being mutable
		//  - not having getter fields and constructors
		// Potentially we should be able to use protobuf serializer itself here
//    return new ProtobufKryoSerializer()
		return new KryoSerializer<>(getTypeClass(), config);
	}
}
