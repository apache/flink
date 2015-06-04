/*
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
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

/**
 * {@link StreamOperator} for streaming sources.
 */
public class StreamSource<OUT> extends AbstractUdfStreamOperator<OUT, SourceFunction<OUT>> implements StreamOperator<OUT> {

	private static final long serialVersionUID = 1L;

	public StreamSource(SourceFunction<OUT> sourceFunction) {
		super(sourceFunction);

		this.chainingStrategy = ChainingStrategy.HEAD;
	}

	public void run(final Object lockingObject, final Collector<OUT> collector) throws Exception {
		SourceFunction.SourceContext<OUT> ctx = new SourceFunction.SourceContext<OUT>() {
			@Override
			public void collect(OUT element) {
				collector.collect(element);
			}

			@Override
			public Object getCheckpointLock() {
				return lockingObject;
			}
		};

		userFunction.run(ctx);
	}

	public void cancel() {
		userFunction.cancel();
	}
}
