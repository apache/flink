package eu.stratosphere.pact.runtime.iterative.concurrent;

import com.google.common.collect.Maps;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;

/** a concurrent datastructure that allows the handover of an object between a pair of threads*/
public class Broker<K, V> {

  private final ConcurrentMap<K, HandOver<V>> mediations = Maps.newConcurrentMap();

  /** threadsafe call to get a shared {@link HandOver} object */
  public HandOver<V> mediate(K key) {
    HandOver<V> handOver = new HandOver<V>();
    HandOver<V> commonHandOver = mediations.putIfAbsent(key, handOver);
    return commonHandOver != null ? commonHandOver : handOver;
  }

  /** remove {@link HandOver} object after successful delivery */
  public void notifyHandOverDone(K key) {
    mediations.remove(key);
  }

  /** can be used for a blocking hand over of an object to share */
  static class HandOver<V> {

    /** single element queue for executing the hand over */
    private final BlockingQueue<V> queue = new ArrayBlockingQueue<V>(1);

    /** provision of the object to share */
    public void handIn(V obj) {
      queue.offer(obj);
    }

    /** blocking retrieval of the object to share */
    public V get() {
      try {
        return queue.take();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
