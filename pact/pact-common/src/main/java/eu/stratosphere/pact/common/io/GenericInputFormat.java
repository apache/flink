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

package eu.stratosphere.pact.common.io;


import static eu.stratosphere.pact.common.util.ReflectionUtil.getTemplateType1;
import static eu.stratosphere.pact.common.util.ReflectionUtil.getTemplateType2;

import java.io.IOException;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.template.GenericInputSplit;
import eu.stratosphere.pact.common.io.statistics.BaseStatistics;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;

/**
 * Generic base class for all inputs that are not based on files. 
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public abstract class GenericInputFormat<K extends Key, V extends Value> implements InputFormat<GenericInputSplit, K, V>
{
	/**
	 * The input key type class.
	 */
	protected Class<K> keyClass;

	/**
	 * The input value type class.
	 */
	protected Class<V> valueClass;
	
	/**
	 * The partition of this split.
	 */
	protected int partitionNumber;
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Default constructor initializing the input data types.
	 */
	protected GenericInputFormat()
	{
		this.keyClass = getTemplateType1(getClass());
		this.valueClass = getTemplateType2(getClass());
	}

	// --------------------------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.io.InputFormat#configure(eu.stratosphere.nephele.configuration.Configuration)
	 */
	@Override
	public void configure(Configuration parameters)
	{
		//	nothing by default
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.io.InputFormat#getStatistics()
	 */
	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics)
	{
		// no statistics available, by default.
		return cachedStatistics;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.io.InputFormat#createInputSplits(int)
	 */
	@Override
	public GenericInputSplit[] createInputSplits(int minNumSplits) throws IOException
	{
		GenericInputSplit[] splits = new GenericInputSplit[minNumSplits];
		for (int i = 0; i < splits.length; i++) {
			splits[i] = new GenericInputSplit(i);
		}
		return splits;
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.io.InputFormat#getInputSplitType()
	 */
	@Override
	public Class<GenericInputSplit> getInputSplitType()
	{
		return GenericInputSplit.class;
	}

	// --------------------------------------------------------------------------------------------

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.io.InputFormat#open(eu.stratosphere.nephele.template.InputSplit)
	 */
	@Override
	public void open(GenericInputSplit split) throws IOException
	{
		this.partitionNumber = split.getSplitNumber();		
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.io.InputFormat#close()
	 */
	@Override
	public void close() throws IOException
	{
		// nothing by default 	
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.io.InputFormat#createPair()
	 */
	@Override
	public KeyValuePair<K, V> createPair()
	{
		try {
			return new KeyValuePair<K, V>(
					this.keyClass.newInstance(),
					this.valueClass.newInstance());
		}
		catch (IllegalAccessException iaex) {
			throw new RuntimeException("Could not access key or value class, or their default (nullary) constructor.");
		}
		catch (InstantiationException iex) {
			throw new RuntimeException("Could not instantiate key or value class. " +
					"Type classes are abstract, or the nullary constructor is missing.");
		}
	}

}
