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

/**
 * <p>
 * Base class for memory backed objects. This class should be implemented by all objects depending on (possibly more
 * than one) {@link MemorySegment}s during their life span.
 * </p>
 * <p>
 * {@code MemoryBacked} objects are injected with the required {@code
 * MemorySegment}s from an external memory management component via the {@link MemoryBacked#bind(MemorySegment)} method.
 * A bound {@code MemoryBacked} object is fully functional at least until the backing memory is exhausted (in which case
 * an {@link OutOfMemoryException} is thrown or some other indication is given to the client), or the client unbinds the
 * attached memory via the {@link MemoryBacked#unbind()} method. A method call of a public method functional only in a
 * bound state results in an {@link UnboundMemoryBackedException}.
 * </p>
 * <p>
 * Please note that memory access is generally not concurrent, i.e. possible inconsistencies caused by concurrent access
 * to the same memory segment must be handled by the client.
 * </p>
 * 
 * @author Alexander Alexandrov
 */
abstract public class MemoryBacked {

	/**
	 * The underlying memory segment.
	 */
	protected MemorySegment memory;

	/**
	 * A boolean flag indicating whether the buffer is bound to an underlying
	 * memory or not.
	 */
	protected boolean isBound;

	// -------------------------------------------------------------------------
	// Constructors / Destructors
	// -------------------------------------------------------------------------

	public MemoryBacked() {
		this.memory = null;
		this.isBound = false;
	}

	// -------------------------------------------------------------------------
	// API
	// -------------------------------------------------------------------------

	/**
	 * Binds this memory backed object to the provided memory segment. If the
	 * memory backed object is already bound, the operation has no effects.
	 * 
	 * @return boolean value indicating whether the operation caused any changes
	 */
	public boolean bind(MemorySegment memory) {
		if (!isBound()) {
			this.memory = memory;
			this.isBound = true;
		}

		return this.isBound;
	}

	/**
	 * Releases the memory segment bound to this object. Returns the released {@code MemorySegment}. Throws an
	 * {@link UnboundMemoryBackedException} if
	 * the object is not bound.
	 * 
	 * @return the released {@code MemorySegment}
	 * @throws UnboundMemoryBackedException
	 */
	public MemorySegment unbind() throws UnboundMemoryBackedException {
		if (!isBound()) {
			throw new UnboundMemoryBackedException();
		}

		MemorySegment memory = this.memory;

		this.memory = null;
		this.isBound = false;

		return memory;
	}

	/**
	 * Checks if the buffer is bound.
	 * 
	 * @return true if the buffer is bound
	 */
	public final boolean isBound() {
		return isBound;
	}
}
