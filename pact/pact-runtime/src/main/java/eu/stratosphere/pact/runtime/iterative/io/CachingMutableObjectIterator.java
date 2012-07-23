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

import com.google.common.base.Preconditions;
import eu.stratosphere.nephele.services.memorymanager.DataInputView;
import eu.stratosphere.pact.common.generic.types.TypeSerializer;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.runtime.io.SpillingBuffer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.EOFException;
import java.io.IOException;

public class CachingMutableObjectIterator<T> implements MutableObjectIterator<T> {

  private final MutableObjectIterator<T> delegate;
  private final SpillingBuffer spillingBuffer;
  private final TypeSerializer<T> typeSerializer;
  private final String name;

  private DataInputView cache;

  private static final Log log = LogFactory.getLog(CachingMutableObjectIterator.class);

  public CachingMutableObjectIterator(MutableObjectIterator<T> delegate, SpillingBuffer spillingBuffer,
      TypeSerializer<T> typeSerializer, String name) {
    this.delegate = delegate;
    this.spillingBuffer = spillingBuffer;
    this.typeSerializer = typeSerializer;
    this.name = name;
  }

  private boolean isCached() {
    return cache != null;
  }

  //TODO remove counting once code is stable
  private int recordsRead = 0;

  private boolean readFromDelegateAndCache(T record) throws IOException {

    log.info("CachingIterator of " + name + " waiting for record("+ (recordsRead) +")");

    boolean recordFound = delegate.next(record);
    if (recordFound) {
      log.info("CachingIterator of " + name + " read and cached record("+ (recordsRead) +")");
      recordsRead++;
      typeSerializer.serialize(record, spillingBuffer);
    } else {
      log.info("CachingIterator of " + name + " releases input");
    }
    return recordFound;
  }

  public void enableReading() throws IOException {
    log.info("CachingIterator of " + name + " enables cache reading");
    cache = Preconditions.checkNotNull(spillingBuffer.flip());
  }

  private boolean readFromCache(T record) throws IOException {

    log.info("CachingIterator of " + name + " waiting for record("+ (recordsRead) +") in cache mode");
    try {
      recordsRead++;
      typeSerializer.deserialize(record, cache);

      log.info("CachingIterator of " + name + " read record("+ (recordsRead) +") in cache mode");
      return true;
    } catch (EOFException e) {
      return false;
    }
  }

  @Override
  public boolean next(T record) throws IOException {
    return isCached() ? readFromCache(record) : readFromDelegateAndCache(record);
  }
}
