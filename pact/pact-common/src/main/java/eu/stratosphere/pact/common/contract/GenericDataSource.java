/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.common.contract;

import java.lang.annotation.Annotation;

import eu.stratosphere.pact.common.stubs.StubAnnotation;
import eu.stratosphere.pact.common.util.Visitor;
import eu.stratosphere.pact.generic.contract.Contract;
import eu.stratosphere.pact.generic.io.InputFormat;

/**
 * Abstract superclass for data sources in a Pact plan.
 * 
 * @param T The type of input format invoked by instances of this data source.
 */
public class GenericDataSource<T extends InputFormat<?, ?>> extends Contract implements StubAnnotationConfigurable {
	
	private static final String DEFAULT_NAME = "<Unnamed Generic Data Source>";
	
	protected final Class<? extends T> clazz;
	
	protected String statisticsKey;

	/**
	 * Creates a new instance for the given file using the given input format.
	 * 
	 * @param clazz The Class for the specific input format
	 * @param name The given name for the Pact, used in plans, logs and progress messages.
	 */
	public GenericDataSource(Class<? extends T> clazz, String name) {
		super(name);
		this.clazz = clazz;
	}
	
	/**
	 * Creates a new instance for the given file using the given input format, using the default name.
	 * 
	 * @param clazz The Class for the specific input format
	 */
	public GenericDataSource(Class<? extends T> clazz) {
		super(DEFAULT_NAME);
		this.clazz = clazz;
	}

	// --------------------------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.recordcontract.OutputContractConfigurable#addOutputContract(java.lang.Class)
	 */
	@Override
	public void addStubAnnotation(Class<? extends Annotation> oc) {
		if (!oc.getEnclosingClass().equals(StubAnnotation.class)) {
			throw new IllegalArgumentException("The given annotation does not describe an output contract.");
		}

		this.ocs.add(oc);
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.recordcontract.OutputContractConfigurable#getOutputContracts()
	 */
	@Override
	public Class<? extends Annotation>[] getStubAnnotation() {
		@SuppressWarnings("unchecked")
		Class<? extends Annotation>[] targetArray = new Class[this.ocs.size()];
		return (Class<? extends Annotation>[]) this.ocs.toArray(targetArray);
	}
	
	/**
	 * Gets the class describing the input format.
	 * 
	 * @return The class describing the input format.
	 */
	public Class<? extends T> getFormatClass() {
		return this.clazz;
	}
	
	/**
	 * Gets the class describing the input format.
	 * <p>
	 * This method is basically identical to {@link #getFormatClass()}.
	 * 
	 * @return The class describing the input format.
	 * 
	 * @see eu.stratosphere.pact.generic.contract.Contract#getUserCodeClass()
	 */
	@Override
	public Class<?> getUserCodeClass() {
		return this.clazz;
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Gets the key under which statistics about this data source may be obtained from the
	 * statistics cache.
	 * 
	 * @return The statistics cache key.
	 */
	public String getStatisticsKey() {
		return this.statisticsKey;
	}
	
	/**
	 * Sets the key under which statistics about this data source may be obtained from the
	 * statistics cache. Useful for testing purposes, when providing mock statistics.
	 * 
	 * @param statisticsKey The key for the statistics object.
	 */
	public void setStatisticsKey(String statisticsKey) {
		this.statisticsKey = statisticsKey;
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Accepts the visitor and applies it this instance. Since the data sources have no inputs, no recursive descend
	 * happens. The visitors pre-visit method is called and, if returning <tt>true</tt>, the post-visit method is called.
	 * 
	 * @param visitor The visitor.
	 *  
	 * @see eu.stratosphere.pact.common.util.Visitable#accept(eu.stratosphere.pact.common.util.Visitor)
	 */
	@Override
	public void accept(Visitor<Contract> visitor) {
		if (visitor.preVisit(this)) {
			visitor.postVisit(this);
		}
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		return this.name;
	}
}
