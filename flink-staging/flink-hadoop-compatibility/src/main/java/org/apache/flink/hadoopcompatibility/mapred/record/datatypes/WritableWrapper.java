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


package org.apache.flink.hadoopcompatibility.mapred.record.datatypes;

import java.io.IOException;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Value;
import org.apache.flink.util.InstantiationUtil;
import org.apache.hadoop.io.Writable;

public class WritableWrapper<T extends Writable> implements Value {
	private static final long serialVersionUID = 2L;
	
	private T wrapped;
	private String wrappedType;
	private ClassLoader cl;
	
	public WritableWrapper() {
	}
	
	public WritableWrapper(T toWrap) {
		wrapped = toWrap;
		wrappedType = toWrap.getClass().getCanonicalName();
	}

	public T value() {
		return wrapped;
	}
	
	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeUTF(wrappedType);
		wrapped.write(out);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		if(cl == null) {
			cl = Thread.currentThread().getContextClassLoader();
		}
		wrappedType = in.readUTF();
		try {
			@SuppressWarnings("unchecked")
			Class<T> wrClass = (Class<T>) Class.forName(wrappedType, true, cl).asSubclass(Writable.class);
			wrapped = InstantiationUtil.instantiate(wrClass, Writable.class);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Error creating the WritableWrapper", e);
		}
		wrapped.readFields(in);
	}

}
