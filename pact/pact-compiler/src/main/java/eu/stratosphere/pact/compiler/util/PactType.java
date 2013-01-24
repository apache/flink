/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.compiler.util;

import eu.stratosphere.pact.common.contract.CoGroupContract;
import eu.stratosphere.pact.common.contract.CrossContract;
import eu.stratosphere.pact.common.contract.GenericDataSink;
import eu.stratosphere.pact.common.contract.GenericDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.generic.contract.Contract;

/**
 * An enumeration describing the type of the PACT.
 */
public enum PactType
{
	Cogroup(CoGroupContract.class),
	Cross(CrossContract.class),
	DataSource(GenericDataSource.class),
	DataSink(GenericDataSink.class),
	Map(MapContract.class),
	Match(MatchContract.class),
	Reduce(ReduceContract.class),
	Union(Contract.class);

	private Class<? extends Contract> clazz; // The class describing the contract

	/**
	 * Private constructor to set enum attributes.
	 * 
	 * @param clazz
	 *        The class of the actual PACT contract represented by this enum constant.
	 */
	private PactType(Class<? extends Contract> clazz) {
		this.clazz = clazz;
	}

	/**
	 * Gets the class of the actual PACT contract represented by this enum constant.
	 * 
	 * @return The class of the actual PACT contract.
	 */
	public Class<? extends Contract> getPactClass() {
		return this.clazz;
	}

	/**
	 * Utility method that gets the enum constant for a PACT class.
	 * 
	 * @param pactClass
	 *        The PACT class to find the enum constant for.
	 * @return The enum constant for the given pact class.
	 */
	public static PactType getType(Class<? extends Contract> pactClass) {
		PactType[] values = PactType.values();
		for (int i = 0; i < values.length; i++) {
			if (values[i].clazz.isAssignableFrom(pactClass)) {
				return values[i];
			}
		}
		return null;
	}
}