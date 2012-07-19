package eu.stratosphere.pact.runtime.iterative.io;

import eu.stratosphere.pact.common.util.MutableObjectIterator;

import java.io.IOException;

public class CachingMutableObjectIterator<T> implements MutableObjectIterator<T> {

  private final MutableObjectIterator<T> delegate;
  //private final SpillingBuffer spillingBuffer;
  //private final TypeSerializer<T> typeSerializer;
  //private DataInputView cache;

  public CachingMutableObjectIterator(MutableObjectIterator<T> delegate) {
    this.delegate = delegate;
  }

  @Override
  public boolean next(T target) throws IOException {
    boolean recordFound = delegate.next(target);
    if (recordFound) {
      System.out.println("caching record...");
    }
    return recordFound;
    /*if (!cached) {

      if (recordFound) {
        typeSerializer.serialize(target, spillingBuffer);
      } else {
        cached = true;
        cache = spillingBuffer.flip();
      }
      return recordFound;
    } else {
      try {
        typeSerializer.deserialize(target, cache);
        return true;
      } catch (EOFException eofex) {
        //TODO works only once
        return false;
      }
    }               */

  }
}
