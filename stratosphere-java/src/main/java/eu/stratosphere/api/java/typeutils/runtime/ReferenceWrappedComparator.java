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

import java.io.IOException;
import java.io.Serializable;

import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.api.common.typeutils.TypeComparatorFactory;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;
import eu.stratosphere.core.memory.MemorySegment;
import eu.stratosphere.util.InstantiationUtil;
import eu.stratosphere.util.Reference;


public class ReferenceWrappedComparator<T> extends TypeComparator<Reference<T>> implements Serializable {

	private static final long serialVersionUID = 1L;
	
	
	private final TypeComparator<T> comparator;
	
	
	public ReferenceWrappedComparator(TypeComparator<T> comparator) {
		this.comparator = comparator;
	}

	public TypeComparator<T> getWrappedComparator() {
		return this.comparator;
	}
	
	@Override
	public int hash(Reference<T> record) {
		return comparator.hash(record.ref);
	}

	@Override
	public void setReference(Reference<T> toCompare) {
		comparator.setReference(toCompare.ref);
	}

	@Override
	public boolean equalToReference(Reference<T> candidate) {
		return comparator.equalToReference(candidate.ref);
	}

	@Override
	public int compareToReference(TypeComparator<Reference<T>> referencedComparator) {
		return comparator.compareToReference(((ReferenceWrappedComparator<T>) referencedComparator).comparator);
	}

	@Override
	public int compare(DataInputView firstSource, DataInputView secondSource) throws IOException {
		return comparator.compare(firstSource, secondSource);
	}

	@Override
	public boolean supportsNormalizedKey() {
		return comparator.supportsNormalizedKey();
	}

	@Override
	public boolean supportsSerializationWithKeyNormalization() {
		return false;
	}

	@Override
	public int getNormalizeKeyLen() {
		return comparator.getNormalizeKeyLen();
	}

	@Override
	public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
		return comparator.isNormalizedKeyPrefixOnly(keyBytes);
	}

	@Override
	public void putNormalizedKey(Reference<T> record, MemorySegment target, int offset, int numBytes) {
		comparator.putNormalizedKey(record.ref, target, offset, numBytes);
	}

	@Override
	public void writeWithKeyNormalization(Reference<T> record, DataOutputView target) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Reference<T> readWithKeyDenormalization(Reference<T> reuse, DataInputView source) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean invertNormalizedKey() {
		return comparator.invertNormalizedKey();
	}

	@Override
	public ReferenceWrappedComparator<T> duplicate() {
		return new ReferenceWrappedComparator<T>(comparator.duplicate());
	}
	
	// --------------------------------------------------------------------------------------------
	// --------------------------------------------------------------------------------------------
	
	
	public static final class ReferenceWrappedComparatorFactory<T> implements TypeComparatorFactory<Reference<T>>, java.io.Serializable {

		private static final long serialVersionUID = 1L;
		

		private static final String CONFIG_KEY = "SER_DATA";
		
		private ReferenceWrappedComparator<T> comparator;
		
		
		public ReferenceWrappedComparatorFactory() {}
		
		public ReferenceWrappedComparatorFactory(ReferenceWrappedComparator<T> comparator) {
			this.comparator = comparator;
		}
		
		@Override
		public void writeParametersToConfig(Configuration config) {
			try {
				InstantiationUtil.writeObjectToConfig(comparator, config, CONFIG_KEY);
			}
			catch (Exception e) {
				throw new RuntimeException("Could not serialize comparator into the configuration.", e);
			}
		}

		@SuppressWarnings("unchecked")
		@Override
		public void readParametersFromConfig(Configuration config, ClassLoader cl) throws ClassNotFoundException {
			try {
				comparator = (ReferenceWrappedComparator<T>) InstantiationUtil.readObjectFromConfig(config, CONFIG_KEY, cl);
			}
			catch (ClassNotFoundException e) {
				throw e;
			}
			catch (Exception e) {
				throw new RuntimeException("Could not serialize serializer into the configuration.", e);
			}
		}

		@Override
		public ReferenceWrappedComparator<T> createComparator() {
			if (comparator != null) {
				return comparator;
			} else {
				throw new RuntimeException("ComparatorFactory ahas not been initialized from configuration.");
			}
		}
	}
}
