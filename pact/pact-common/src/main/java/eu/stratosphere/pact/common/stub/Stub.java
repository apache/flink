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

package eu.stratosphere.pact.common.stub;

import java.io.Closeable;
import java.io.IOException;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.Value;

/**
 * Abstract stub class for all PACT stubs. PACT stubs must be overwritten to
 * provide user implementations for PACT programs.
 * 
 * @author Fabian Hueske
 * @param <OK>
 *        Type of the output key
 * @param <OV>
 *        Type of the output value
 */
public abstract class Stub<OK extends Key, OV extends Value> implements Closeable {

	/**
	 * Output key type.
	 */
	protected Class<OK> ok;

	/**
	 * Output value type.
	 */
	protected Class<OV> ov;

	/**
	 * Default constructor. Initializes the input and output types of the stub.
	 */
	public Stub() {
		initTypes();
	}

	/**
	 * Initializes the key and value types of the stubs input and output.
	 */
	protected abstract void initTypes();

	/**
	 * Configures the stub. This method is called before the run() method is
	 * invoked. The method receives a Configuration object which holds
	 * parameters that were passed to the stub during plan construction. This
	 * method should be used to evaluate these parameters and configure the user
	 * stub implementation.
	 * 
	 * @see eu.stratosphere.nephele.configuration.Configuration
	 * @param parameters
	 */
	public abstract void configure(Configuration parameters);

	/**
	 * Is called after configure() and before run().
	 * This method should be used for initial setup of the stub implementation.
	 * 
	 * @throws IOException
	 *         if an I/O error occurred
	 */
	public abstract void open() throws IOException;

	/**
	 * Is called after run().
	 * This method should be used for clean up.
	 * 
	 * @throws IOException
	 *         if an I/O error occurred
	 */
	public abstract void close() throws IOException;

	/**
	 * Returns the type of the output key.
	 * 
	 * @return Type of the output key.
	 */
	public Class<OK> getOutKeyType() {
		return ok;
	}

	/**
	 * Returns the type of the output value.
	 * 
	 * @return Type of the output value.
	 */
	public Class<OV> getOutValueType() {
		return ov;
	}

}
