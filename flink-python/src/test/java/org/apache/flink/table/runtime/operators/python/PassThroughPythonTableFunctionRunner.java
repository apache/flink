/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.operators.python;

import org.apache.flink.table.runtime.runners.python.PythonTableFunctionRunner;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.beam.sdk.fn.data.FnDataReceiver;

/**
 * A {@link PythonTableFunctionRunner} that just emit each input element.
 */
public class PassThroughPythonTableFunctionRunner extends AbstractPassThroughPythonTableFunctionRunner<Row> {
	PassThroughPythonTableFunctionRunner(FnDataReceiver<Row> resultReceiver) {
		super(resultReceiver);
	}

	@Override
	public Row copy(Row element) {
		return Row.copy(element);
	}

	@Override
	public void finishBundle() throws Exception {
		Preconditions.checkState(bundleStarted);
		bundleStarted = false;

		for (Row element : bufferedElements) {
			resultReceiver.accept(element);
			resultReceiver.accept(new Row(0));
		}
		bufferedElements.clear();
	}
}
