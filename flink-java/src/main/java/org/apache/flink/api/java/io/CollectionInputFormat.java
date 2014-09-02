/**
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


package org.apache.flink.api.java.io;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.flink.api.common.io.GenericInputFormat;
import org.apache.flink.api.common.io.NonParallelInput;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.memory.InputViewObjectInputStreamWrapper;
import org.apache.flink.core.memory.OutputViewObjectOutputStreamWrapper;

/**
 * An input format that returns objects from a collection.
 */
public class CollectionInputFormat<T> extends GenericInputFormat<T> implements NonParallelInput {

	private static final long serialVersionUID = 1L;

	private TypeSerializer<T> serializer;

	private transient Collection<T> dataSet; // input data as collection. transient, because it will be serialized in a custom way
	
	private transient Iterator<T> iterator;

	
	public CollectionInputFormat(Collection<T> dataSet, TypeSerializer<T> serializer) {
		if (dataSet == null) {
			throw new NullPointerException();
		}

		this.serializer = serializer;
		
		this.dataSet = dataSet;
	}

	
	@Override
	public boolean reachedEnd() throws IOException {
		return !this.iterator.hasNext();
	}

	@Override
	public void open(GenericInputSplit split) throws IOException {
		super.open(split);
		
		this.iterator = this.dataSet.iterator();
	}
	
	@Override
	public T nextRecord(T record) throws IOException {
		return this.iterator.next();
	}

	// --------------------------------------------------------------------------------------------

	private void writeObject(ObjectOutputStream out) throws IOException {
		out.defaultWriteObject();
		out.writeInt(dataSet.size());
		
		for (T element : dataSet){
			serializer.serialize(element, new OutputViewObjectOutputStreamWrapper(out));
		}
	}

	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		in.defaultReadObject();

		int collectionLength = in.readInt();
		List<T> list = new ArrayList<T>(collectionLength);
		

		for (int i = 0; i < collectionLength; i++){
			T element = serializer.createInstance();
			element = serializer.deserialize(element, new InputViewObjectInputStreamWrapper(in));
			list.add(element);
		}

		dataSet = list;
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public String toString() {
		return this.dataSet.toString();
	}
	
	// --------------------------------------------------------------------------------------------
	
	public static <X> void checkCollection(Collection<X> elements, Class<X> viewedAs) {
		if (elements == null || viewedAs == null) {
			throw new NullPointerException();
		}
		
		for (X elem : elements) {
			if (elem == null) {
				throw new IllegalArgumentException("The collection must not contain null elements.");
			}
			
			if (!viewedAs.isAssignableFrom(elem.getClass())) {
				throw new IllegalArgumentException("The elements in the collection are not all subclasses of " + 
							viewedAs.getCanonicalName());
			}
		}
	}
}
