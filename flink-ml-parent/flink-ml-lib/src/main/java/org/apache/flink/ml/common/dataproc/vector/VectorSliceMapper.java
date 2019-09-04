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
import org.apache.flink.ml.common.mapper.SISOMapper;
import org.apache.flink.ml.common.utils.VectorTypes;
import org.apache.flink.ml.params.dataproc.vector.VectorSliceParams;
import org.apache.flink.table.api.TableSchema;

/**
 * This mapper maps vector to a thinner one with special indices.
 */
public class VectorSliceMapper extends SISOMapper {

	private int[] indices;

	public VectorSliceMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		this.indices = this.params.get(VectorSliceParams.INDICES);
	}

	@Override
	protected Object map(Object input) {
		if (input == null) {
			return null;
		}

		Vector vec = (Vector) input;
		return vec.slice(indices);
	}

	@Override
	protected TypeInformation initOutputColType() {
		return VectorTypes.VECTOR;
	}

}
