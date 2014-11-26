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

package org.apache.flink.api.java.operators.translation;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.operators.base.MapOperatorBase;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;

public class PlanProjectOperator<T, R extends Tuple> extends MapOperatorBase<T, R, MapFunction<T, R>> {

	public PlanProjectOperator(int[] fields, String name, TypeInformation<T> inType, TypeInformation<R> outType, ExecutionConfig executionConfig) {
		super(new MapProjector<T, R>(fields, outType.createSerializer(executionConfig).createInstance()), new UnaryOperatorInformation<T, R>(inType, outType), name);
	}
	
	public static final class MapProjector<T, R extends Tuple>
		extends AbstractRichFunction
		implements MapFunction<T, R>
	{
		private static final long serialVersionUID = 1L;
		
		private final int[] fields;
		private final R outTuple;
		
		private MapProjector(int[] fields, R outTupleInstance) {
			this.fields = fields;
			this.outTuple = outTupleInstance;
		}

		// TODO We should use code generation for this.
		@Override
		public R map(T inTuple) throws Exception {
			
			for(int i=0; i<fields.length; i++) {
				outTuple.setField(((Tuple)inTuple).getField(fields[i]), i);
			}
			return outTuple;
		}
	}
}
