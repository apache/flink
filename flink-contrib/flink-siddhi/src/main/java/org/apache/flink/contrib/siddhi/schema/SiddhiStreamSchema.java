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
import org.apache.flink.contrib.siddhi.utils.SiddhiTypeFactory;
import org.apache.flink.util.Preconditions;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import java.util.ArrayList;
import java.util.List;

/**
 * Siddhi specific Stream Schema.
 *
 * @param <T> Siddhi stream element type
 */
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
			streamDefinition.attribute(getFieldNames()[i], SiddhiTypeFactory.getAttributeType(getFieldTypes()[i]));
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
}
