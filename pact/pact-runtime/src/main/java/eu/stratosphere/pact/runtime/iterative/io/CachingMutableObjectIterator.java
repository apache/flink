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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.EOFException;
import java.io.IOException;

/**
 * A {@link MutableObjectIterator} that reads its input into a {@link SpillingBuffer}, so that it can be read again later.
 *
 * The contract for this class is as following
 *
 * After the initial input is consumed (when next() returned false for the first time), the caller has to invoke enableReading() prior to
 * reading the cached data and must read the cached data fully afterwards
 *
 */
public class CachingMutableObjectIterator<T> implements MutableObjectIterator<T> {

  /** the original input */
  private final MutableObjectIterator<T> delegate;
  /** the buffer to cache the input in, will spill to disk in case of overflow */
  private final SpillingBuffer spillingBuffer;
  /** serializer for the input data */
  private final TypeSerializer<T> typeSerializer;
  /** name of the owning task for logging purposes */
  private final String name;

  /** a {@link java.io.DataInput} to read the cached data from */
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

//  private int recordsRead = 0;

  /** read records from original input and write them to the cache */
  private boolean readFromDelegateAndCache(T record) throws IOException {

//    log.info("CachingIterator of " + name + " waiting for record("+ (recordsRead) +")");

    boolean recordFound = delegate.next(record);
    if (recordFound) {
//      log.info("CachingIterator of " + name + " read and cached record("+ (recordsRead) +")");
//      recordsRead++;
      typeSerializer.serialize(record, spillingBuffer);
    } else if (log.isInfoEnabled()) {
      log.info("CachingIterator of " + name + " releases input");
    }
    return recordFound;
  }

  /** must be called prior to reading cached data */
  public void enableReading() throws IOException {
    if (log.isInfoEnabled()) {
      log.info("CachingIterator of " + name + " enables cache reading");
    }
    cache = spillingBuffer.flip();
  }

  /** reread input from the cache */
  private boolean readFromCache(T record) throws IOException {

//    log.info("CachingIterator of " + name + " waiting for record("+ (recordsRead) +") in cache mode");
    try {
//      recordsRead++;
      typeSerializer.deserialize(record, cache);

//      log.info("CachingIterator of " + name + " read record("+ (recordsRead) +") in cache mode");
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
