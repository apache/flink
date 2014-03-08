/***********************************************************************************************************************
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
 **********************************************************************************************************************/

package eu.stratosphere.hadoopcompatibility.datatypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import eu.stratosphere.types.Value;
import eu.stratosphere.util.InstantiationUtil;

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

	public <X extends Writable> X value() {
		return (X) wrapped;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(wrappedType);
		wrapped.write(out);
	}

	@Override
	public void read(DataInput in) throws IOException {
		if(cl == null) {
			cl = Thread.currentThread().getContextClassLoader();
		}
		wrappedType = in.readUTF();
		try {
			Class wrClass = Class.forName(wrappedType, true, cl);
			wrapped = (T) InstantiationUtil.instantiate(wrClass, Writable.class);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Error creating the WritableWrapper", e);
		}
		wrapped.readFields(in);
	}

}
