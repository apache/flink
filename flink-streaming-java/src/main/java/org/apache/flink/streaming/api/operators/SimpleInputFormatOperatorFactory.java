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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.streaming.api.functions.source.InputFormatSourceFunction;

/**
 * Input format source operator factory which just wrap existed {@link StreamSource}.
 *
 * @param <OUT> The output type of the operator
 */
@Internal
public class SimpleInputFormatOperatorFactory<OUT> extends SimpleOperatorFactory<OUT> implements InputFormatOperatorFactory<OUT> {

	private final StreamSource<OUT, InputFormatSourceFunction<OUT>> operator;

	public SimpleInputFormatOperatorFactory(StreamSource<OUT, InputFormatSourceFunction<OUT>> operator) {
		super(operator);
		this.operator = operator;
	}

	@Override
	public InputFormat<OUT, InputSplit> getInputFormat() {
		return operator.getUserFunction().getFormat();
	}
}
