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

package eu.stratosphere.pact.runtime.iterative.io;

import eu.stratosphere.nephele.services.memorymanager.DataInputView;
import eu.stratosphere.pact.common.generic.types.TypeSerializer;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.runtime.io.SpillingBuffer;

import java.io.EOFException;
import java.io.IOException;

public class CachingMutableObjectIterator<T> implements MutableObjectIterator<T> {

  private final MutableObjectIterator<T> delegate;
  private final SpillingBuffer spillingBuffer;
  private final TypeSerializer<T> typeSerializer;

  private DataInputView cache;

  public CachingMutableObjectIterator(MutableObjectIterator<T> delegate, SpillingBuffer spillingBuffer,
      TypeSerializer<T> typeSerializer) {
    this.delegate = delegate;
    this.spillingBuffer = spillingBuffer;
    this.typeSerializer = typeSerializer;
  }

  private boolean isCached() {
    return cache != null;
  }

  private boolean readFromDelegateAndCache(T record) throws IOException{
    boolean recordFound = delegate.next(record);
    if (recordFound) {
      System.out.println("caching record...");
      typeSerializer.serialize(record, spillingBuffer);
    } else {
      // input is completely read, flip the buffer
      cache = spillingBuffer.flip();
    }
    return recordFound;
  }

  private boolean readFromCache(T record) throws IOException {
    return false;
    /*try {
      typeSerializer.deserialize(record, cache);
      return true;
    } catch (EOFException eofex) {
      //TODO reset cache
      return false;
    }     */
  }

  @Override
  public boolean next(T record) throws IOException {
    return isCached() ? readFromCache(record) : readFromDelegateAndCache(record);
  }
}
