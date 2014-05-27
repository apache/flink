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
package eu.stratosphere.api.java.typeutils.runtime;

import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.api.common.typeutils.TypePairComparator;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;


public class PojoPairComparator<T1, T2> extends TypePairComparator<T1, T2> implements Serializable {

	private static final long serialVersionUID = 1L;

	private final int[] keyPositions1, keyPositions2;
	private transient Field[] keyFields1, keyFields2;
	private final TypeComparator<Object>[] comparators1;
	private final TypeComparator<Object>[] comparators2;

	@SuppressWarnings("unchecked")
	public PojoPairComparator(int[] keyPositions1, Field[] keyFields1, int[] keyPositions2, Field[] keyFields2, TypeComparator<Object>[] comparators1, TypeComparator<Object>[] comparators2) {

		if(keyPositions1.length != keyPositions2.length
			|| keyPositions1.length != comparators1.length
			|| keyPositions2.length != comparators2.length) {

			throw new IllegalArgumentException("Number of key fields and comparators differ.");
		}

		int numKeys = keyPositions1.length;

		this.keyPositions1 = keyPositions1;
		this.keyPositions2 = keyPositions2;
		this.keyFields1 = keyFields1;
		this.keyFields2 = keyFields2;
		this.comparators1 = new TypeComparator[numKeys];
		this.comparators2 = new TypeComparator[numKeys];

		for(int i = 0; i < numKeys; i++) {
			this.comparators1[i] = comparators1[i].duplicate();
			this.comparators2[i] = comparators2[i].duplicate();
		}
	}

	private void writeObject(ObjectOutputStream out)
			throws IOException, ClassNotFoundException {
		out.defaultWriteObject();
		out.writeInt(keyFields1.length);
		for (Field field: keyFields1) {
			out.writeObject(field.getDeclaringClass());
			out.writeUTF(field.getName());
		}
		out.writeInt(keyFields2.length);
		for (Field field: keyFields2) {
			out.writeObject(field.getDeclaringClass());
			out.writeUTF(field.getName());
		}
	}

	private void readObject(ObjectInputStream in)
			throws IOException, ClassNotFoundException {
		in.defaultReadObject();
		int numKeyFields = in.readInt();
		keyFields1 = new Field[numKeyFields];
		for (int i = 0; i < numKeyFields; i++) {
			Class<?> clazz = (Class<?>)in.readObject();
			String fieldName = in.readUTF();
			try {
				keyFields1[i] = clazz.getField(fieldName);
				keyFields1[i].setAccessible(true);
			} catch (NoSuchFieldException e) {
				throw new RuntimeException("Class resolved at TaskManager is not compatible with class read during Plan setup.");
			}
		}
		numKeyFields = in.readInt();
		keyFields2 = new Field[numKeyFields];
		for (int i = 0; i < numKeyFields; i++) {
			Class<?> clazz = (Class<?>)in.readObject();
			String fieldName = in.readUTF();
			try {
				keyFields2[i] = clazz.getField(fieldName);
				keyFields2[i].setAccessible(true);
			} catch (NoSuchFieldException e) {
				throw new RuntimeException("Class resolved at TaskManager is not compatible with class read during Plan setup.");
			}
		}
	}

	@Override
	public void setReference(T1 reference) {
		for(int i=0; i < this.comparators1.length; i++) {
			try {
				this.comparators1[i].setReference(keyFields1[i].get(reference));
			} catch (IllegalAccessException e) {
				throw new RuntimeException("This should not happen since we call setAccesssible(true) in PojoTypeInfo.");
			}
		}
	}

	@Override
	public boolean equalToReference(T2 candidate) {
		for(int i=0; i < this.comparators1.length; i++) {
			try {
				if(!this.comparators1[i].equalToReference(keyFields2[i].get(candidate))) {
					return false;
				}
			} catch (IllegalAccessException e) {
				throw new RuntimeException("Class resolved at TaskManager is not compatible with class read during Plan setup.");
			}
		}
		return true;
	}

	@Override
	public int compareToReference(T2 candidate) {
		for(int i=0; i < this.comparators1.length; i++) {
			try {
				this.comparators2[i].setReference(keyFields2[i].get(candidate));
			} catch (IllegalAccessException e) {
				throw new RuntimeException("Class resolved at TaskManager is not compatible with class read during Plan setup.");
			}
			int res = this.comparators1[i].compareToReference(this.comparators2[i]);
			if(res != 0) {
				return res;
			}
		}
		return 0;
	}



}
