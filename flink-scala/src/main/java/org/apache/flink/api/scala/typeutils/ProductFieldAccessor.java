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

package org.apache.flink.api.scala.typeutils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.InvalidFieldReferenceException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.FieldAccessor;
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializerBase;
import scala.Product;

import static org.apache.flink.util.Preconditions.checkNotNull;

public final class ProductFieldAccessor<T, R, F> extends FieldAccessor<T, F> {

	private static final long serialVersionUID = 1L;

	private final int pos;
	private final TupleSerializerBase<T> serializer;
	private final Object[] fields;
	private final int length;
	private final FieldAccessor<R, F> innerAccessor;

	ProductFieldAccessor(int pos, TypeInformation<T> typeInfo, FieldAccessor<R, F> innerAccessor, ExecutionConfig config) {
		int arity = ((TupleTypeInfoBase)typeInfo).getArity();
		if(pos < 0 || pos >= arity) {
			throw new InvalidFieldReferenceException(
				"Tried to select " + ((Integer) pos).toString() + ". field on \"" +
					typeInfo.toString() + "\", which is an invalid index.");
		}
		checkNotNull(typeInfo, "typeInfo must not be null.");
		checkNotNull(innerAccessor, "innerAccessor must not be null.");

		this.pos = pos;
		this.fieldType = ((TupleTypeInfoBase<T>)typeInfo).getTypeAt(pos);
		this.serializer = (TupleSerializerBase<T>)typeInfo.createSerializer(config);
		this.length = this.serializer.getArity();
		this.fields = new Object[this.length];
		this.innerAccessor = innerAccessor;
	}

	@SuppressWarnings("unchecked")
	@Override
	public F get(T record) {
		return innerAccessor.get((R)((Product)record).productElement(pos));
	}

	@SuppressWarnings("unchecked")
	@Override
	public T set(T record, F fieldValue) {
		Product prod = (Product)record;
		for (int i = 0; i < length; i++) {
			fields[i] = prod.productElement(i);
		}
		fields[pos] = innerAccessor.set((R)fields[pos], fieldValue);
		return serializer.createInstance(fields);
	}
}
