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

package org.apache.flink.ml.common.dataproc.vector;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.common.linalg.Vector;
import org.apache.flink.ml.common.mapper.SISOFlatMapper;
import org.apache.flink.ml.common.utils.VectorTypes;
import org.apache.flink.ml.params.dataproc.vector.VectorSizeHintParams;
import org.apache.flink.ml.params.shared.HasSize;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.util.Collector;

/**
 * This mapper checks the size of vector and give results as parameter define.
 */
public class VectorSizeHintMapper extends SISOFlatMapper {
	private int size;

	private enum HandleType {
		/**
		 * If the input vector is null or its size does not match the given one, then throw exception.
		 */
		ERROR,
		/**
		 * It will accept the vector if it is not null and its size matches the given one.
		 */
		SKIP,
		/**
		 * It will accept the vector if the vector is not null.
		 */
		OPTIMISTIC
	}

	private HandleType handleMethod;

	public VectorSizeHintMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		this.handleMethod = HandleType.valueOf(this.params.get(VectorSizeHintParams.HANDLE_INVALID).toUpperCase());
		this.size = this.params.get(HasSize.SIZE);
	}

	@Override
	protected void flatMap(Object input, Collector<Object> output) {
		Vector vec;
		switch (handleMethod) {
			case SKIP:
				if (null == input) {
					return;
				} else {
					vec = (Vector) input;
					if (vec.size() == size) {
						output.collect(vec);
					}
				}
				break;
			case ERROR:
				if (input == null) {
					throw new NullPointerException(
						"Got null vector in VectorSizeHint, set `handleInvalid` to 'skip' to filter invalid rows.");
				} else {
					vec = (Vector) input;
					if (vec.size() == size) {
						output.collect(vec);
					} else {
						throw new IllegalArgumentException(
							"VectorSizeHint : vec size (" + vec.size() + ") not equal param size (" + size + ").");
					}
				}
				break;
			case OPTIMISTIC:
				if (input != null) {
					output.collect(input);
				}
		}
	}

	@Override
	protected TypeInformation initOutputColType() {
		return VectorTypes.VECTOR;
	}
}
