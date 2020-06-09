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

package org.apache.flink.table.runtime.keyselector;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.generated.Projection;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;

/**
 * A KeySelector which will extract key from RowData. The key type is BinaryRowData.
 */
public class BinaryRowDataKeySelector implements RowDataKeySelector {

	private static final long serialVersionUID = 5375355285015381919L;

	private final RowDataTypeInfo keyRowType;
	private final GeneratedProjection generatedProjection;
	private transient Projection<RowData, BinaryRowData> projection;

	public BinaryRowDataKeySelector(RowDataTypeInfo keyRowType, GeneratedProjection generatedProjection) {
		this.keyRowType = keyRowType;
		this.generatedProjection = generatedProjection;
	}

	@Override
	public RowData getKey(RowData value) throws Exception {
		if (projection == null) {
			ClassLoader cl = Thread.currentThread().getContextClassLoader();
			//noinspection unchecked
			projection = generatedProjection.newInstance(cl);
		}
		return projection.apply(value).copy();
	}

	@Override
	public RowDataTypeInfo getProducedType() {
		return keyRowType;
	}

}
