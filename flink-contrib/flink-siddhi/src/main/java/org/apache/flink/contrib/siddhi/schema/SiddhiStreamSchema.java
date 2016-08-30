/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.siddhi.schema;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Preconditions;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SiddhiStreamSchema<T> extends StreamSchema<T> {
	private static final String DEFINE_STREAM_TEMPLATE = "define stream %s (%s);";

	public SiddhiStreamSchema(TypeInformation<T> typeInfo, String... fieldNames) {
		super(typeInfo, fieldNames);
	}

	public SiddhiStreamSchema(TypeInformation<T> typeInfo, int[] fieldIndexes, String[] fieldNames) {
		super(typeInfo, fieldIndexes, fieldNames);
	}

	public StreamDefinition getStreamDefinition(String streamId) {
		StreamDefinition streamDefinition = StreamDefinition.id(streamId);
		for (int i = 0; i < getFieldNames().length; i++) {
			streamDefinition.attribute(getFieldNames()[i], getAttributeType(getFieldTypes()[i]));
		}
		return streamDefinition;
	}

	public String getStreamDefinitionExpression(StreamDefinition streamDefinition) {
		List<String> columns = new ArrayList<>();
		Preconditions.checkNotNull(streamDefinition, "StreamDefinition is null");
		for (Attribute attribute : streamDefinition.getAttributeList()) {
			columns.add(String.format("%s %s", attribute.getName(), attribute.getType().toString().toLowerCase()));
		}
		return String.format(DEFINE_STREAM_TEMPLATE, streamDefinition.getId(), StringUtils.join(columns, ","));
	}

	public String getStreamDefinitionExpression(String streamId) {
		StreamDefinition streamDefinition = getStreamDefinition(streamId);
		List<String> columns = new ArrayList<>();
		Preconditions.checkNotNull(streamDefinition, "StreamDefinition is null");
		for (Attribute attribute : streamDefinition.getAttributeList()) {
			columns.add(String.format("%s %s", attribute.getName(), attribute.getType().toString().toLowerCase()));
		}
		return String.format(DEFINE_STREAM_TEMPLATE, streamDefinition.getId(), StringUtils.join(columns, ","));
	}

	/**
	 * TODO: Decouple attribute type mapping to external class
	 */
	private static <F> Attribute.Type getAttributeType(TypeInformation<F> fieldType) {
		if (SIDDHI_TYPE_MAPPING.containsKey(fieldType.getTypeClass())) {
			return SIDDHI_TYPE_MAPPING.get(fieldType.getTypeClass());
		} else {
			return Attribute.Type.OBJECT;
		}
	}

	private final static Map<Class<?>, Attribute.Type> SIDDHI_TYPE_MAPPING = new HashMap<>();

	static {
		SIDDHI_TYPE_MAPPING.put(String.class, Attribute.Type.STRING);
		SIDDHI_TYPE_MAPPING.put(Integer.class, Attribute.Type.INT);
		SIDDHI_TYPE_MAPPING.put(int.class, Attribute.Type.INT);
		SIDDHI_TYPE_MAPPING.put(Long.class, Attribute.Type.LONG);
		SIDDHI_TYPE_MAPPING.put(long.class, Attribute.Type.LONG);
		SIDDHI_TYPE_MAPPING.put(Float.class, Attribute.Type.FLOAT);
		SIDDHI_TYPE_MAPPING.put(float.class, Attribute.Type.FLOAT);
		SIDDHI_TYPE_MAPPING.put(Double.class, Attribute.Type.DOUBLE);
		SIDDHI_TYPE_MAPPING.put(double.class, Attribute.Type.DOUBLE);
		SIDDHI_TYPE_MAPPING.put(Boolean.class, Attribute.Type.BOOL);
		SIDDHI_TYPE_MAPPING.put(boolean.class, Attribute.Type.BOOL);
	}
}
