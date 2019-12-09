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

import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.generated.Projection;
import org.apache.flink.table.runtime.typeutils.BaseRowTypeInfo;

/**
 * A KeySelector which will extract key from BaseRow. The key type is BinaryRow.
 */
public class BinaryRowKeySelector implements BaseRowKeySelector {

	private static final long serialVersionUID = 5375355285015381919L;

	private final BaseRowTypeInfo keyRowType;
	private final GeneratedProjection generatedProjection;
	private transient Projection<BaseRow, BinaryRow> projection;

	public BinaryRowKeySelector(BaseRowTypeInfo keyRowType, GeneratedProjection generatedProjection) {
		this.keyRowType = keyRowType;
		this.generatedProjection = generatedProjection;
	}

	@Override
	public BaseRow getKey(BaseRow value) throws Exception {
		if (projection == null) {
			ClassLoader cl = Thread.currentThread().getContextClassLoader();
			//noinspection unchecked
			projection = generatedProjection.newInstance(cl);
		}
		return projection.apply(value).copy();
	}

	@Override
	public BaseRowTypeInfo getProducedType() {
		return keyRowType;
	}

}
