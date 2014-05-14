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
package eu.stratosphere.api.common.typeutils.base;

import java.io.IOException;

import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;


public abstract class BasicTypeComparator<T extends Comparable<T>> extends TypeComparator<T> implements java.io.Serializable {

	private static final long serialVersionUID = 1L;
	
	private transient T reference;
	
	protected final boolean ascendingComparison;
	

	protected BasicTypeComparator(boolean ascending) {
		this.ascendingComparison = ascending;
	}

	@Override
	public int hash(T value) {
		return value.hashCode();
	}

	@Override
	public void setReference(T toCompare) {
		this.reference = toCompare;
	}

	@Override
	public boolean equalToReference(T candidate) {
		return candidate.equals(reference);
	}

	@Override
	public int compareToReference(TypeComparator<T> referencedComparator) {
		int comp = ((BasicTypeComparator<T>) referencedComparator).reference.compareTo(reference);
		return ascendingComparison ? comp : -comp;
	}

	@Override
	public int compare(T first, T second) {
		int cmp = first.compareTo(second);
		return ascendingComparison ? cmp : -cmp;
	}

	@Override
	public boolean invertNormalizedKey() {
		return !ascendingComparison;
	}
	
	@Override
	public boolean supportsSerializationWithKeyNormalization() {
		return false;
	}

	@Override
	public void writeWithKeyNormalization(T record, DataOutputView target) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public T readWithKeyDenormalization(T reuse, DataInputView source) throws IOException {
		throw new UnsupportedOperationException();
	}
}
