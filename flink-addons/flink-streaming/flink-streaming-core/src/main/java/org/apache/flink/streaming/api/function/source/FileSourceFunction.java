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

package org.apache.flink.streaming.api.function.source;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.operators.GenericDataSourceBase;
import org.apache.flink.api.common.operators.OperatorInformation;
import org.apache.flink.api.common.operators.util.UserCodeWrapper;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.api.java.typeutils.runtime.RuntimeStatefulSerializerFactory;
import org.apache.flink.api.java.typeutils.runtime.RuntimeStatelessSerializerFactory;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.util.Collector;

public class FileSourceFunction extends SourceFunction<String> {
	private static final long serialVersionUID = 1L;

	private InputSplitProvider provider;

	private InputFormat<String, ?> format;

	private TypeSerializerFactory<String> serializerFactory;

	private UserCodeWrapper<? extends InputFormat<String, ?>> formatWrapper;

	// cancel flag
	private volatile boolean taskCanceled = false;

	public FileSourceFunction(InputFormat<String, ?> format, TypeInformation<String> typeInfo) {
		this.format = format;
		@SuppressWarnings({ "unchecked", "rawtypes" })
		GenericDataSourceBase<String, ?> source = new GenericDataSourceBase(format,
				new OperatorInformation<String>(typeInfo), format.toString());
		formatWrapper = source.getUserCodeWrapper();
		this.serializerFactory = createSerializer(typeInfo);
	}

	@Override
	public UserCodeWrapper<? extends InputFormat<String, ?>> getFormatWrapper() {
		return this.formatWrapper;
	}

	private static TypeSerializerFactory<String> createSerializer(TypeInformation<String> typeInfo) {
		TypeSerializer<String> serializer = typeInfo.createSerializer();

		if (serializer.isStateful()) {
			return new RuntimeStatefulSerializerFactory<String>(serializer, typeInfo.getTypeClass());
		} else {
			return new RuntimeStatelessSerializerFactory<String>(serializer,
					typeInfo.getTypeClass());
		}
	}

	@Override
	public void invoke(Collector<String> collector) throws Exception {
		final TypeSerializer<String> serializer = serializerFactory.getSerializer();
		final Iterator<InputSplit> splitIterator = getInputSplits();
		@SuppressWarnings("unchecked")
		final InputFormat<String, InputSplit> format = (InputFormat<String, InputSplit>) this.format;
		try {
			while (!this.taskCanceled && splitIterator.hasNext()) {

				final InputSplit split = splitIterator.next();
				String record = serializer.createInstance();

				format.open(split);
				try {
					while (!this.taskCanceled && !format.reachedEnd()) {
						if ((record = format.nextRecord(record)) != null) {
							collector.collect(record);
						}
					}
				} finally {
					format.close();
				}
			}
			collector.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
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
	public final void initialize(Environment env) {
		this.provider = env.getInputSplitProvider();
	}

}
