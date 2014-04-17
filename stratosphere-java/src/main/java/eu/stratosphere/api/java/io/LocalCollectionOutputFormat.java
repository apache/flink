/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.api.java.io;


import eu.stratosphere.api.common.io.OutputFormat;
import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.api.java.typeutils.InputTypeConfigurable;
import eu.stratosphere.api.java.typeutils.TypeInformation;
import eu.stratosphere.configuration.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 *  An output format that writes record into collection
 */
public class LocalCollectionOutputFormat<T> implements OutputFormat<T>, InputTypeConfigurable {

	private static final long serialVersionUID = 1L;

	private static Map<Integer,Collection<?>> RESULT_HOLDER = new HashMap<Integer, Collection<?>>();

	private transient ArrayList<T> taskResult;

	private TypeSerializer<T> typeSerializer;

	private int id;

	public LocalCollectionOutputFormat(Collection<T> out) {
		synchronized (RESULT_HOLDER) {
			this.id = generateRandomId();
			RESULT_HOLDER.put(this.id, out);
		}
	}

	private int generateRandomId() {
		int num = (int) (Math.random() * Integer.MAX_VALUE);
		while (RESULT_HOLDER.containsKey(num)) {
			num = (int) (Math.random() * Integer.MAX_VALUE);
		}
		return num;
	}

	@Override
	public void configure(Configuration parameters) {}


	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		this.taskResult = new ArrayList<T>();
	}

	@Override
	public void writeRecord(T record) throws IOException {
		T recordCopy = this.typeSerializer.createInstance();
		recordCopy = this.typeSerializer.copy(record, recordCopy);
		this.taskResult.add(recordCopy);
	}


	@Override
	public void close() throws IOException {
		synchronized (RESULT_HOLDER) {
			@SuppressWarnings("unchecked")
			Collection<T> result = (Collection<T>) RESULT_HOLDER.get(this.id);
			result.addAll(this.taskResult);
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public void setInputType(TypeInformation<?> type) {
		this.typeSerializer = (TypeSerializer<T>)type.createSerializer();
	}
}
