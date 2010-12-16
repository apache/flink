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

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.io.InputFormat;
import eu.stratosphere.pact.common.plan.Visitor;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.Value;

/**
 * Contract for input nodes which read data from external data sources.
 * 
 * @author DIMA
 */
public class DataSourceContract<KT extends Key, VT extends Value> extends Contract implements
		OutputContractConfigurable {
	private static String defaultName = "DataSource #";

	private static int nextID = 1;

	private Class<? extends Annotation> outputContract; // the output contract class

	protected final Class<? extends InputFormat<KT, VT>> clazz;

	protected final String filePath;

	protected final Configuration formatParameters;

	/**
	 * Creates a new instance for the given file using the given input format.
	 * 
	 * @param clazz
	 *        class for the specific input format
	 * @param file
	 *        input location
	 * @param name
	 *        name for the node
	 */
	public DataSourceContract(Class<? extends InputFormat<KT, VT>> clazz, String file, String name) {
		super(name);

		this.clazz = clazz;
		filePath = file;
		formatParameters = new Configuration();
	}

	/**
	 * Creates a new instance for the given file using the given input format.
	 * 
	 * @param clazz
	 *        class for the specific input format
	 * @param file
	 *        input location
	 */
	public DataSourceContract(Class<? extends InputFormat<KT, VT>> c, String file) {
		this(c, file, defaultName + (nextID++));
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void setOutputContract(Class<? extends Annotation> oc) {
		if (!oc.getEnclosingClass().equals(OutputContract.class)) {
			throw new IllegalArgumentException("The given annotation does not describe an output contract.");
		}

		this.outputContract = oc;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Class<? extends Annotation> getOutputContract() {
		return this.outputContract;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void accept(Visitor<Contract> visitor) {
		if (visitor.preVisit(this)) {
			visitor.postVisit(this);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Class<? extends InputFormat<KT, VT>> getStubClass() {
		return clazz;
	}

	/**
	 * Returns the file path from which the input is read.
	 * 
	 * @return
	 */
	public String getFilePath() {
		return filePath;
	}

	/**
	 * Configure the settings for the output format.
	 * 
	 * @param key
	 * @param value
	 */
	public void setFormatParameter(String key, String value) {
		formatParameters.setString(key, value);
	}

	/**
	 * Returns the parameters set for the OutputFormat
	 * 
	 * @return
	 */
	public Configuration getFormatParameters() {
		return formatParameters;
	}
}
