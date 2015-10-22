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

package org.apache.flink.api.scala.operators;


import com.google.common.base.Preconditions;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.java.io.CommonCsvInputFormat;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializerBase;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.StringUtils;

public class ScalaCsvInputFormat<OUT> extends CommonCsvInputFormat<OUT> {
	private static final long serialVersionUID = -7347888812778968640L;

	private final TupleSerializerBase<OUT> tupleSerializer;

	public ScalaCsvInputFormat(Path filePath, CompositeType<OUT> typeInfo) {
		super(filePath, typeInfo);

		Preconditions.checkArgument(typeInfo instanceof PojoTypeInfo || typeInfo instanceof TupleTypeInfoBase,
			"Only pojo types or tuple types are supported.");

		if (typeInfo instanceof TupleTypeInfoBase) {
			TupleTypeInfoBase<OUT> tupleTypeInfo = (TupleTypeInfoBase<OUT>) typeInfo;

			tupleSerializer = (TupleSerializerBase<OUT>)tupleTypeInfo.createSerializer(new ExecutionConfig());
		} else {
			tupleSerializer = null;
		}
	}

	@Override
	protected OUT createTuple(OUT reuse) {
		Preconditions.checkNotNull(tupleSerializer, "The tuple serializer must be initialised." +
			" It is not initialized if the given type was not a " +
			TupleTypeInfoBase.class.getName() + ".");

		return tupleSerializer.createInstance(parsedValues);
	}

	@Override
	public String toString() {
		return "Scala CSV Input (" + StringUtils.showControlCharacters(String.valueOf(getFieldDelimiter())) + ") " + getFilePath();
	}
}
