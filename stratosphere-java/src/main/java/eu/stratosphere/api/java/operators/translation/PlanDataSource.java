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
package eu.stratosphere.api.java.operators.translation;

import java.io.IOException;

import eu.stratosphere.api.common.io.InputFormat;
import eu.stratosphere.api.common.io.statistics.BaseStatistics;
import eu.stratosphere.api.common.operators.GenericDataSource;
import eu.stratosphere.api.java.typeutils.TypeInformation;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.io.InputSplit;
import eu.stratosphere.util.Reference;


public class PlanDataSource<T> extends GenericDataSource<InputFormat<Reference<T>,?>> implements JavaPlanNode<T> {

	private final TypeInformation<T> producedType;

	public PlanDataSource(InputFormat<T, ?> format, String name, TypeInformation<T> producedType) {
		super(createWrapper(format), name);
		
		this.producedType = producedType;
	}
	

	@Override
	public TypeInformation<T> getReturnType() {
		return producedType;
	}
	
	@SuppressWarnings("unchecked")
	private static <T, S extends InputSplit> ReferenceWrappingInputFormat<T, S> createWrapper(InputFormat<T, ?> format) {
		return new ReferenceWrappingInputFormat<T, S>((InputFormat<T, S>) format);
	}
	
	// --------------------------------------------------------------------------------------------
	// --------------------------------------------------------------------------------------------
	
	public static final class ReferenceWrappingInputFormat<T, S extends InputSplit> implements InputFormat<Reference<T>, S> {

		private static final long serialVersionUID = 1L;

		private final InputFormat<T, S> format;
		
		private final Reference<T> ref = new Reference<T>();
		
		
		public ReferenceWrappingInputFormat(InputFormat<T, S> format) {
			this.format = format;
		}


		@Override
		public void configure(Configuration parameters) {
			format.configure(parameters);
		}

		@Override
		public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
			return format.getStatistics(cachedStatistics);
		}

		@Override
		public S[] createInputSplits(int minNumSplits) throws IOException {
			return format.createInputSplits(minNumSplits); 
		}

		@Override
		public Class<? extends S> getInputSplitType() {
			return format.getInputSplitType();
		}

		@Override
		public void open(S split) throws IOException {
			format.open(split);
		}

		@Override
		public boolean reachedEnd() throws IOException {
			return format.reachedEnd();
		}

		@Override
		public Reference<T> nextRecord(Reference<T> reuse) throws IOException {
			T value = format.nextRecord(reuse.ref);
			if (value != null) {
				ref.ref = value;
				return ref;
			} else {
				return null;
			}
		}

		@Override
		public void close() throws IOException {
			format.close();
		}
	}
}
