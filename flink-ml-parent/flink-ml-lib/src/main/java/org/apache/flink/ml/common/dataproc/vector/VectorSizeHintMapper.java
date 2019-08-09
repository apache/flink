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
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.common.linalg.Vector;
import org.apache.flink.ml.common.mapper.SISOFlatMapper;
import org.apache.flink.ml.params.dataproc.vector.VectorSizeHintParams;
import org.apache.flink.ml.params.shared.HasSize;
import org.apache.flink.table.api.TableSchema;

import java.util.List;

/**
 * This mapper check the size of vector and give results as parameter define.
 */
public class VectorSizeHintMapper extends SISOFlatMapper {
	private int size;

	private enum HandleType {
		ERROR,
		SKIP,
		OPTIMISTIC
	}

	private HandleType handleMethod;

	public VectorSizeHintMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		this.handleMethod = HandleType.valueOf(this.params.get(VectorSizeHintParams.HANDLE_INVALID).toUpperCase());
		this.size = this.params.get(HasSize.SIZE);
	}

	@Override
	protected void flatMap(Object input, List<Object> output) {
		String in = (String) input;
		switch (handleMethod) {
			case SKIP:
				if (in == null) {
					return;
				} else {
					if (getVectorSize(in) == size) {
						output.add(in);
					}
				}
				break;
			case ERROR:
				if (in == null) {
					throw new NullPointerException(
						"Got null vector in VectorSizeHint, set `handleInvalid` to 'skip' to filter invalid rows.");
				} else {
					int localSize = getVectorSize(in);
					if (localSize == size) {
						output.add(in);
					} else {
						throw new RuntimeException(
							"VectorSizeHint : vec size (" + localSize + ") not equal param size (" + size + ").");
					}
				}
				break;
			case OPTIMISTIC:
				if (in == null || in.isEmpty()) {
					return;
				} else {
					output.add(in);
				}

		}
	}

	private int getVectorSize(String in) {
		Vector vec = Vector.parse(in);
		return vec.size();
	}

	@Override
	protected TypeInformation initOutputColType() {
		return Types.STRING;
	}
}
