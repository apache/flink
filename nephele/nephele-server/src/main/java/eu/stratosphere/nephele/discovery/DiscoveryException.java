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

package eu.stratosphere.nephele.discovery;

/**
 * Exception that is thrown when an error related to the {@link DiscoveryService}.
 * 
 * @author warneke
 */
public class DiscoveryException extends Exception {

	/**
	 * Random serialization ID.
	 */
	private static final long serialVersionUID = 7647491334281738439L;

  public DiscoveryException(String msg) {
    super(msg);
  }

	/**
	 * Constructs a new discovery exception w
	 * 
	 * @param cause the   error that should b e transported with this exceptiopn
	 */
  public DiscoveryException(Throwable cause) {
    super(cause);
  }
}
