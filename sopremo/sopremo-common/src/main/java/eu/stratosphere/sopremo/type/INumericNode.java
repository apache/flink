/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.sopremo.type;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Interface for all nodes that store numerical values.
 * 
 * @author Michael Hopstock
 * @author Tommy Neubert
 */
public interface INumericNode extends IPrimitiveNode {

	/**
	 * Returns this nodes value as an <code>int</code>.
	 */
	public abstract int getIntValue();

	/**
	 * Returns this nodes value as a <code>long</code>.
	 */
	public abstract long getLongValue();

	/**
	 * Returns this nodes value as a {@link BigInteger}.
	 */
	public abstract BigInteger getBigIntegerValue();

	/**
	 * Returns this nodes value as a {@link BigDecimal}.
	 */
	public abstract BigDecimal getDecimalValue();

	/**
	 * Returns this nodes value as a <code>double</code>.
	 */
	public abstract double getDoubleValue();

	/**
	 * Returns the String representation of this nodes value.
	 */
	public abstract String getValueAsText();

	/**
	 * Returns either this node represents a floating point number or not.
	 */
	public abstract boolean isFloatingPointNumber();

	/**
	 * Returns either this node represents an integral number or not.
	 */
	public abstract boolean isIntegralNumber();
}