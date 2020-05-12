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

package org.apache.flink.table.runtime.utils;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.table.runtime.runners.python.table.PythonTableFunctionRunner;
import org.apache.flink.util.Preconditions;

import org.apache.beam.sdk.fn.data.FnDataReceiver;

import java.util.ArrayList;
import java.util.List;

/**
 * A {@link PythonTableFunctionRunner} that just emit each input element.
 */
public abstract class PassThroughPythonTableFunctionRunner<IN> implements PythonFunctionRunner<IN> {
	private boolean bundleStarted;
	private final List<IN> bufferedElements;
	private final FnDataReceiver<byte[]> resultReceiver;

	/**
	 * Reusable OutputStream used to holding the serialized input elements.
	 */
	private transient ByteArrayOutputStreamWithPos baos;

	/**
	 * OutputStream Wrapper.
	 */
	private transient DataOutputViewStreamWrapper baosWrapper;

	public PassThroughPythonTableFunctionRunner(FnDataReceiver<byte[]> resultReceiver) {
		this.resultReceiver = Preconditions.checkNotNull(resultReceiver);
		bundleStarted = false;
		bufferedElements = new ArrayList<>();
	}

	@Override
	public void open() {
		baos = new ByteArrayOutputStreamWithPos();
		baosWrapper = new DataOutputViewStreamWrapper(baos);
	}

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
		int num = 0;

		for (IN element : bufferedElements) {
			num++;
			baos.reset();
			getInputTypeSerializer().serialize(element, baosWrapper);
			// test for left join
			if (num != 6 && num != 8) {
				resultReceiver.accept(baos.toByteArray());
			}
			resultReceiver.accept(new byte[]{0});
		}
		bufferedElements.clear();
	}

	@Override
	public void processElement(IN element) {
		bufferedElements.add(copy(element));
	}

	public abstract IN copy(IN element);

	public abstract TypeSerializer<IN> getInputTypeSerializer();
}
