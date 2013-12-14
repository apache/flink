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

package eu.stratosphere.api.operators;

import java.lang.annotation.Annotation;

import eu.stratosphere.api.functions.StubAnnotation;
import eu.stratosphere.api.io.InputFormat;
import eu.stratosphere.api.operators.util.StubAnnotationConfigurable;
import eu.stratosphere.api.operators.util.UserCodeClassWrapper;
import eu.stratosphere.api.operators.util.UserCodeObjectWrapper;
import eu.stratosphere.api.operators.util.UserCodeWrapper;
import eu.stratosphere.util.Visitor;

/**
 * Abstract superclass for data sources in a Pact plan.
 * 
 * @param T The type of input format invoked by instances of this data source.
 */
public class GenericDataSource<T extends InputFormat<?, ?>> extends Contract implements StubAnnotationConfigurable {
	
	private static final String DEFAULT_NAME = "<Unnamed Generic Data Source>";
	
	protected final UserCodeWrapper<? extends T> formatWrapper;
	
	protected String statisticsKey;

	/**
	 * Creates a new instance for the given file using the given input format.
	 * 
	 * @param format The {@link InputFormat} implementation used to read the data.
	 * @param name The given name for the Pact, used in plans, logs and progress messages.
	 */
	public GenericDataSource(T format, String name) {
		super(name);
		
		if (format == null)
			throw new IllegalArgumentException("Input format may not be null.");
		
		this.formatWrapper = new UserCodeObjectWrapper<T>(format);
	}
	
	/**
	 * Creates a new instance for the given file using the given input format, using the default name.
	 * 
	 * @param format The {@link InputFormat} implementation used to read the data.
	 */
	public GenericDataSource(T format) {
		super(DEFAULT_NAME);
		
		if (format == null)
			throw new IllegalArgumentException("Input format may not be null.");
		
		this.formatWrapper = new UserCodeObjectWrapper<T>(format);
	}
	
	/**
	 * Creates a new instance for the given file using the given input format.
	 * 
	 * @param format The {@link InputFormat} implementation used to read the data.
	 * @param name The given name for the Pact, used in plans, logs and progress messages.
	 */
	public GenericDataSource(Class<? extends T> format, String name) {
		super(name);
		
		if (format == null)
			throw new IllegalArgumentException("Input format may not be null.");
		
		this.formatWrapper = new UserCodeClassWrapper<T>(format);
	}
	
	/**
	 * Creates a new instance for the given file using the given input format, using the default name.
	 * 
	 * @param format The {@link InputFormat} implementation used to read the data.
	 */
	public GenericDataSource(Class<? extends T> format) {
		super(DEFAULT_NAME);
		
		if (format == null)
			throw new IllegalArgumentException("Input format may not be null.");
		
		this.formatWrapper = new UserCodeClassWrapper<T>(format);
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
	public UserCodeWrapper<? extends T> getFormatWrapper() {
		return this.formatWrapper;
	}
	
	/**
	 * Gets the class describing the input format.
	 * <p>
	 * This method is basically identical to {@link #getFormatWrapper()}.
	 * 
	 * @return The class describing the input format.
	 * 
	 * @see eu.stratosphere.api.operators.Contract#getUserCodeWrapper()
	 */
	@Override
	public UserCodeWrapper<? extends T> getUserCodeWrapper() {
		return this.formatWrapper;
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
	 * @see eu.stratosphere.util.Visitable#accept(eu.stratosphere.util.Visitor)
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
