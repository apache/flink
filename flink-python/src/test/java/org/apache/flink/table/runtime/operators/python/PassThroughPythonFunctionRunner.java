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

import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.util.Preconditions;

import org.apache.beam.sdk.fn.data.FnDataReceiver;

import java.util.ArrayList;
import java.util.List;

/**
 * A {@link PythonFunctionRunner} that just emit each input element.
 *
 * @param <IN> Type of the input elements.
 */
public abstract class PassThroughPythonFunctionRunner<IN> implements PythonFunctionRunner<IN> {

	private boolean bundleStarted;
	private final List<IN> bufferedElements;
	private final FnDataReceiver<IN> resultReceiver;

	PassThroughPythonFunctionRunner(FnDataReceiver<IN> resultReceiver) {
		this.resultReceiver = Preconditions.checkNotNull(resultReceiver);
		bundleStarted = false;
		bufferedElements = new ArrayList<>();
	}

	@Override
	public void open() {}

	@Override
	public void close() {}

	@Override
	public void startBundle() {
		Preconditions.checkState(!bundleStarted);
		bundleStarted = true;
	}

	@Override
	public void finishBundle() throws Exception {
		Preconditions.checkState(bundleStarted);
		bundleStarted = false;

		for (IN element : bufferedElements) {
			resultReceiver.accept(element);
		}
		bufferedElements.clear();
	}

	@Override
	public void processElement(IN element) {
		bufferedElements.add(copy(element));
	}

	public abstract IN copy(IN element);
}
