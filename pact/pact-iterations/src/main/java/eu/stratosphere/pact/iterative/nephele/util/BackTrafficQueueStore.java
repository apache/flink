package eu.stratosphere.pact.iterative.nephele.util;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;

public class BackTrafficQueueStore {
  private volatile HashMap<String, BlockingQueue<Object>> iterOpenMap =
      new HashMap<String, BlockingQueue<Object>>();
  private volatile HashMap<String, BlockingQueue<Object>> iterEndMap =
      new HashMap<String, BlockingQueue<Object>>();

  private static final BackTrafficQueueStore store = new BackTrafficQueueStore();

  public static BackTrafficQueueStore getInstance() {
    return store;
  }

  public void addStructures(JobID jobID, int subTaskId) {
    synchronized(iterOpenMap) {
      synchronized(iterEndMap) {
        if (iterOpenMap.containsKey(getIdentifier(jobID, subTaskId))) {
          throw new RuntimeException("Internal Error");
        }

        BlockingQueue<Object> openQueue =
            new ArrayBlockingQueue<Object>(1);
        BlockingQueue<Object> endQueue =
            new ArrayBlockingQueue<Object>(1);

        iterOpenMap.put(getIdentifier(jobID, subTaskId), openQueue);
        iterEndMap.put(getIdentifier(jobID, subTaskId), endQueue);
      }
    }
  }

  public void releaseStructures(JobID jobID, int subTaskId) {
    synchronized(iterOpenMap) {
      synchronized(iterEndMap) {
        if (!iterOpenMap.containsKey(getIdentifier(jobID, subTaskId))) {
          throw new RuntimeException("Internal Error");
        }

        iterOpenMap.remove(getIdentifier(jobID, subTaskId));
        iterEndMap.remove(getIdentifier(jobID, subTaskId));
      }
    }
  }

  public void publishUpdateBuffer(JobID jobID, int subTaskId, Object buffer) {
    //publishUpdateBuffer(jobID, subTaskId, memorySegments, segmentSize, UpdateQueueStrategy.IN_MEMORY_SERIALIZED);
    BlockingQueue<Object> queue =
         safeRetrieval(iterOpenMap, getIdentifier(jobID, subTaskId));
    if (queue == null) {
      throw new RuntimeException("Internal Error");
    }

    queue.add(buffer);
  }

//  public void publishUpdateBuffer(JobID jobID, int subTaskId, List<MemorySegment> memorySegments, int segmentSize,
//      UpdateQueueStrategy strategy) {
//    BlockingQueue<SerializedUpdateBuffer> queue =
//         safeRetrieval(iterOpenMap, getIdentifier(jobID, subTaskId));
//    if (queue == null) {
//      throw new RuntimeException("Internal Error");
//    }
//
//    queue.add(strategy.getUpdateBufferFactory().createBuffer(jobID, subTaskId, memorySegments, segmentSize));
//  }

  public synchronized Object receiveUpdateBuffer(JobID jobID, int subTaskId) throws InterruptedException {
    BlockingQueue<Object> queue = null;
    int count = 0;
    while (queue == null) {
      queue = safeRetrieval(iterOpenMap, getIdentifier(jobID, subTaskId));
      if (queue == null) {
        Thread.sleep(100);
        count++;
        if (count == 100) {
          throw new RuntimeException("Internal Error");
        }
      }
    }

    return queue.take();
  }

  public void publishIterationEnd(JobID jobID, int subTaskId, Object outputBuffer) {
    BlockingQueue<Object> queue =
         safeRetrieval(iterEndMap, getIdentifier(jobID, subTaskId));
    if (queue == null) {
      throw new RuntimeException("Internal Error");
    }

    queue.add(outputBuffer);
  }

  public Object receiveIterationEnd(JobID jobID, int subTaskId) throws InterruptedException {
    BlockingQueue<Object> queue =
        safeRetrieval(iterEndMap, getIdentifier(jobID, subTaskId));
    if (queue == null) {
      throw new RuntimeException("Internal Error");
    }

    return queue.take();
  }


  private <K,V> V safeRetrieval(HashMap<K, V> map, K key) {
    synchronized(map) {
      return map.get(key);
    }
  }

  private static String getIdentifier(JobID jobID, int subTaskId) {
    return jobID.toString() + "#" + subTaskId;
  }

  private interface UpdateBufferFactory {
    public SerializedUpdateBuffer createBuffer(JobID id, int subTaskId,
        List<MemorySegment> memorySegments, int segmentSize);
  }

//  public enum UpdateQueueStrategy {
//    //IN_MEMORY_OBJECTS(getInstance().new ObjectQueueFactory()),
//    IN_MEMORY_SERIALIZED(getInstance().new SerializingBufferFactory());
//
//    UpdateBufferFactory factory;
//
//    UpdateQueueStrategy(UpdateBufferFactory factory) {
//      this.factory = factory;
//    }
//
//    protected UpdateBufferFactory getUpdateBufferFactory() {
//      return factory;
//    }
//  }

//  private class ObjectQueueFactory implements UpdateQueueFactory {
//    @Override
//    @SuppressWarnings("serial")
//    public Queue<PactRecord> createQueue(JobID id, int subTaskId, int initialSize, final AbstractTask task) {
//      return new ArrayDeque<PactRecord>(initialSize) {
//        @Override
//        public boolean add(PactRecord rec) {
//          PactRecord copy = new PactRecord(rec.getNumFields());
//          rec.copyTo(copy);
//
//          return super.add(copy);
//        }
//
//        @Override
//        public boolean addAll(Collection<? extends PactRecord> c) {
//          throw new UnsupportedOperationException("Not implemented");
//        }
//      };
//    }
//  }

//  private class SerializingBufferFactory implements UpdateBufferFactory {
//    @Override
//    public SerializedUpdateBuffer createBuffer(final JobID jobID, final int subTaskId,
//        List<MemorySegment> memorySegments, int segmentSize) {
//      return new SerializedUpdateBuffer(memorySegments, segmentSize);
//    }
//
//  }
}
