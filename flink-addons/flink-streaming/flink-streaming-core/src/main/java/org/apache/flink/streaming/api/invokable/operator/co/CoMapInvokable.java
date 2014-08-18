/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.flink.streaming.api.invokable.operator.co;

import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.function.co.CoMapFunction;

public class CoMapInvokable<IN1, IN2, OUT> extends CoInvokable<IN1, IN2, OUT> {
	private static final long serialVersionUID = 1L;

	private CoMapFunction<IN1, IN2, OUT> mapper;

	public CoMapInvokable(CoMapFunction<IN1, IN2, OUT> mapper) {
		super(mapper);
		this.mapper = mapper;
	}

	// TODO rework this as UnionRecordReader
	@Override
	public void invoke() throws Exception {
		boolean noMoreRecordOnInput1 = false;
		boolean noMoreRecordOnInput2 = false;

		do {
			noMoreRecordOnInput1 = ((reuse1 = recordIterator1.next(reuse1)) == null);
			if (!noMoreRecordOnInput1) {
				collector.collect(mapper.map1(reuse1.getObject()));
			}

			noMoreRecordOnInput2 = ((reuse2 = recordIterator2.next(reuse2)) == null);
			if (!noMoreRecordOnInput2) {
				collector.collect(mapper.map2(reuse2.getObject()));
			}

			if (!this.isMutable) {
				resetReuse();
			}
		} while (!noMoreRecordOnInput1 && !noMoreRecordOnInput2);
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		if (mapper instanceof RichFunction) {
			((RichFunction) mapper).open(parameters);
		}
	}

	@Override
	public void close() throws Exception {
		if (mapper instanceof RichFunction) {
			((RichFunction) mapper).close();
		}
	}

}
