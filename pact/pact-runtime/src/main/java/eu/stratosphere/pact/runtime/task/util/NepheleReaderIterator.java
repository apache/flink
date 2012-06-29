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

package eu.stratosphere.pact.runtime.task.util;

import java.io.IOException;

import eu.stratosphere.nephele.io.MutableReader;
import eu.stratosphere.pact.common.generic.types.TypeSerializer;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.runtime.plugable.DeserializationDelegate;


/**
 * A {@link MutableObjectIterator} that wraps a Nephele Reader producing records of a certain type.
 *
 * @author Stephan Ewen
 */
public final class NepheleReaderIterator<T> implements MutableObjectIterator<T>
{
	private final MutableReader<DeserializationDelegate<T>> reader;		// the source
	
	private final DeserializationDelegate<T> delegate;

  private final ReaderInterruptionBehavior interruptionBehavior;

	/**
	 * Creates a new iterator, wrapping the given reader.
	 * 
	 * @param reader The reader to wrap.
	 */
	public NepheleReaderIterator(MutableReader<DeserializationDelegate<T>> reader, TypeSerializer<T> serializer)
	{
		this(reader, serializer, ReaderInterruptionBehaviors.EXCEPTION_ON_INTERRUPT);
	}

  /**
   * Creates a new iterator, wrapping the given reader.
   *
   * @param reader The reader to wrap.
   * @param serializer serializer
   * @param interruptionBehavior behavior in case of interruptions
   */
  public NepheleReaderIterator(MutableReader<DeserializationDelegate<T>> reader, TypeSerializer<T> serializer,
      ReaderInterruptionBehavior interruptionBehavior)
  {
    this.reader = reader;
    this.delegate = new DeserializationDelegate<T>(serializer);
    this.interruptionBehavior = interruptionBehavior;
  }

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.util.MutableObjectIterator#next(java.lang.Object)
	 */
	@Override
	public boolean next(T target) throws IOException
	{
		this.delegate.setInstance(target);
		try {
			return this.reader.next(this.delegate);
		}
		catch (InterruptedException iex) {
			return interruptionBehavior.onInterrupt(iex);
		}
	}
}
