package eu.stratosphere.pact.iterative.nephele.util;

import java.util.AbstractQueue;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.collect.UnmodifiableIterator;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.pact.common.type.PactRecord;

public class SerializingQueue extends AbstractQueue<PactRecord> {

  private static final int DEFAULT_MEMORY_SEGMENT_SIZE = 8 * 1024 * 1024;

  List<MemorySegment> segments = Lists.newArrayList();
  List<MemorySegment> newSegments = Lists.newArrayList();
  MemorySegment currentWriteSegment;
  MemorySegment currentReadSegment;
  Iterator<MemorySegment> allocatingIterator;
  MemoryManager memoryManager;
  final int CURRENT_READ_SEGMENT_INDEX = 0;

  PactRecord currentReadRecord = new PactRecord();
  int currentReadOffset = 0;
  boolean readNext = true;

  int count = 0;

  public SerializingQueue(final AbstractInvokable task) {
    memoryManager = task.getEnvironment().getMemoryManager();
    allocatingIterator = new Iterator<MemorySegment>() {
      @Override
      public boolean hasNext() {
        return true;
      }

      @Override
      public MemorySegment next() {
        try {
          return memoryManager.allocateStrict(task, 1, DEFAULT_MEMORY_SEGMENT_SIZE).get(0);
        } catch (Exception ex) {
          throw new RuntimeException("Bad error during serialization", ex);
        }
      }

      @Override
      public void remove() {
      }

    };

    currentWriteSegment = allocatingIterator.next();
    currentReadSegment = currentWriteSegment;
    segments.add(currentWriteSegment);
  }

  @Override
  public boolean offer(PactRecord rec) {
    try {
      rec.serialize(null, currentWriteSegment.outputView, allocatingIterator, newSegments);
    } catch (Exception ex) {
      throw new RuntimeException("Bad error during serialization", ex);
    }

    if (!newSegments.isEmpty()) {
      segments.addAll(newSegments);
      currentWriteSegment = segments.get(segments.size() - 1);
      newSegments.clear();
    }

    count++;

    return true;
  }

  @Override
  public PactRecord peek() {
    if (readNext) {
      int bytesRead = currentReadRecord.deserialize(segments, CURRENT_READ_SEGMENT_INDEX, currentReadOffset);
      while (bytesRead > 0) {
        if (currentReadSegment.size() - currentReadOffset > bytesRead) {
          currentReadOffset += bytesRead;
          bytesRead = 0;
        } else {
          bytesRead -= (currentReadSegment.size() - currentReadOffset);

          //Remove old read segment from list & release in memory manager
          MemorySegment unused = segments.remove(CURRENT_READ_SEGMENT_INDEX);
          memoryManager.release(unused);

          //Update reference to new read segment
          currentReadSegment = segments.get(CURRENT_READ_SEGMENT_INDEX);
          currentReadOffset = 0;
        }
      }
      readNext = false;
    }

    return currentReadRecord;
  }

  @Override
  public PactRecord poll() {
    PactRecord rec = peek();
    readNext = true;
    count--;
    return rec;
  }

  @Override
  public Iterator<PactRecord> iterator() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int size() {
    if (count == 0) {
      //A memory segment can be left if it was not completely full
      if (segments.size() == 1) {
        currentWriteSegment = null;
        memoryManager.release(segments);
      } else if (segments.size() > 1) {
        throw new RuntimeException("Too many memory segments left");
      }
    }
    return count;
  }

  @Override
  public void clear() {
    memoryManager.release(segments);
    segments.clear();
    count = 0;
  }
}
