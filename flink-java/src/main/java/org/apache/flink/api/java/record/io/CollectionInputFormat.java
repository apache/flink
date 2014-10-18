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


package org.apache.flink.api.java.record.io;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import org.apache.flink.api.common.io.GenericInputFormat;
import org.apache.flink.api.common.io.NonParallelInput;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.types.Record;
import org.apache.flink.types.ValueUtil;

/**
 * input format for java collection input. It can accept collection data or serializable iterator
 */
public class CollectionInputFormat extends GenericInputFormat<Record> implements NonParallelInput {

	private static final long serialVersionUID = 1L;

	private Collection<?> dataSet; // input data as collection

	private Iterator<?> serializableIter; // input data as serializable iterator

	private transient Iterator<?> it;

	@Override
	public boolean reachedEnd() throws IOException {
		return !it.hasNext();
	}

	@Override
	public void open(GenericInputSplit split) throws IOException {
		super.open(split);
		if (serializableIter != null) {
			it = serializableIter;
		}
		else {
			it = this.dataSet.iterator();
		}
	}

	@Override
	public Record nextRecord(Record record) throws IOException {
		if (it.hasNext()) {
			record.clear();
			Object b = it.next();
			// check whether the record field is one-dimensional or multi-dimensional
			if (b.getClass().isArray()) {
				for (Object s : (Object[]) b) {
					record.addField(ValueUtil.toFlinkValueType(s));
				}
			}
			else if (b instanceof Collection) {
				@SuppressWarnings("unchecked")
				Iterator<Object> tmpIter = ((Collection<Object>) b).iterator();
				while (tmpIter.hasNext()) {
					Object s = tmpIter.next();
					record.addField(ValueUtil.toFlinkValueType(s));
				}
			}
			else {
				record.setField(0, ValueUtil.toFlinkValueType(b));
			}
			return record;
		} else {
			return null;
		}
	}

	public void setData(Collection<?> data) {
		this.dataSet = data;
		this.serializableIter = null;
	}

	public <T extends Iterator<?>, Serializable> void setIter(T iter) {
		this.serializableIter = iter;
	}
}
