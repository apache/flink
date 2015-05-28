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

package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.streaming.runtime.tasks.StreamingRuntimeContext;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class FileSourceFunction<OUT> extends RichParallelSourceFunction<OUT> {
	private static final long serialVersionUID = 1L;

	private TypeInformation<OUT> typeInfo;
	private transient TypeSerializer<OUT> serializer;

	private InputSplitProvider provider;
	private InputFormat<OUT, InputSplit> format;

	private Iterator<InputSplit> splitIterator;
	private transient OUT nextElement;

	@SuppressWarnings("unchecked")
	public FileSourceFunction(InputFormat<OUT, ?> format, TypeInformation<OUT> typeInfo) {
		this.format = (InputFormat<OUT, InputSplit>) format;
		this.typeInfo = typeInfo;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void open(Configuration parameters) throws Exception {
		StreamingRuntimeContext context = (StreamingRuntimeContext) getRuntimeContext();
		this.provider = context.getInputSplitProvider();
		format.configure(context.getTaskStubParameters());
		serializer = typeInfo.createSerializer(getRuntimeContext().getExecutionConfig());

		splitIterator = getInputSplits();
		if (splitIterator.hasNext()) {
			format.open(splitIterator.next());
		}

	}

	@Override
	public void close() throws Exception {
		super.close();
		format.close();
	}

	private Iterator<InputSplit> getInputSplits() {

		return new Iterator<InputSplit>() {

			private InputSplit nextSplit;

			private boolean exhausted;

			@Override
			public boolean hasNext() {
				if (exhausted) {
					return false;
				}

				if (nextSplit != null) {
					return true;
				}

				InputSplit split = provider.getNextInputSplit();

				if (split != null) {
					this.nextSplit = split;
					return true;
				} else {
					exhausted = true;
					return false;
				}
			}

			@Override
			public InputSplit next() {
				if (this.nextSplit == null && !hasNext()) {
					throw new NoSuchElementException();
				}

				final InputSplit tmp = this.nextSplit;
				this.nextSplit = null;
				return tmp;
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}

	@Override
	public boolean reachedEnd() throws Exception {
		if (nextElement != null) {
			return false;
		}
		nextElement = serializer.createInstance();
		nextElement =  format.nextRecord(nextElement);
		if (nextElement == null && splitIterator.hasNext()) {
			format.open(splitIterator.next());
			return reachedEnd();
		} else if (nextElement == null) {
			return true;
		}
		return false;
	}

	@Override
	public OUT next() throws Exception {
		if (reachedEnd()) {
			throw new RuntimeException("End of FileSource reached.");
		}

		OUT result = nextElement;
		nextElement = null;
		return result;
	}

}
