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

package org.apache.flink.table.runtime.operators.values;

import org.apache.flink.api.common.io.GenericInputFormat;
import org.apache.flink.api.common.io.NonParallelInput;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.runtime.generated.GeneratedInput;
import org.apache.flink.table.runtime.typeutils.BaseRowTypeInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Generated ValuesInputFormat.
 */
public class ValuesInputFormat
		extends GenericInputFormat<BaseRow>
		implements NonParallelInput, ResultTypeQueryable<BaseRow> {

	private static final Logger LOG = LoggerFactory.getLogger(ValuesInputFormat.class);
	private GeneratedInput<GenericInputFormat<BaseRow>> generatedInput;
	private final BaseRowTypeInfo returnType;
	private GenericInputFormat<BaseRow> format;

	public ValuesInputFormat(GeneratedInput<GenericInputFormat<BaseRow>> generatedInput, BaseRowTypeInfo returnType) {
		this.generatedInput = generatedInput;
		this.returnType = returnType;
	}

	@Override
	public void open(GenericInputSplit split) {
		LOG.debug("Compiling GenericInputFormat: $name \n\n Code:\n$code",
				generatedInput.getClassName(), generatedInput.getCode());
		LOG.debug("Instantiating GenericInputFormat.");

		format = generatedInput.newInstance(getRuntimeContext().getUserCodeClassLoader());
		generatedInput = null;
	}

	@Override
	public boolean reachedEnd() throws IOException {
		return format.reachedEnd();
	}

	@Override
	public BaseRow nextRecord(BaseRow reuse) throws IOException {
		return format.nextRecord(reuse);
	}

	@Override
	public BaseRowTypeInfo getProducedType() {
		return returnType;
	}

}
