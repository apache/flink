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

package eu.stratosphere.nephele.services.memorymanager;

import eu.stratosphere.nephele.services.ServiceException;

/**
 * An exception to be thrown when a memory allocation operation is not
 * successful.
 * 
 * @author Alexander Alexandrov
 */
public class MemoryAllocationException extends ServiceException
{
	private static final long serialVersionUID = -403983866457947012L;

	public MemoryAllocationException() {
		super();
	}

	public MemoryAllocationException(String message, Throwable cause) {
		super(message, cause);
	}

	public MemoryAllocationException(String message) {
		super(message);
	}

	public MemoryAllocationException(Throwable cause) {
		super(cause);
	}
}
