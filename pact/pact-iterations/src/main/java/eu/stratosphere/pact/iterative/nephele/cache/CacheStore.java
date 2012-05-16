package eu.stratosphere.pact.iterative.nephele.cache;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class CacheStore {

  public static enum CacheType {
    ISOLATED, SHARED_READ, SHARED_READ_WRITE
  }

  @SuppressWarnings("rawtypes")
  private static final ConcurrentMap<String, ConcurrentMap> STORE = new ConcurrentHashMap<String, ConcurrentMap>();
  private static final ConcurrentMap<String, Set<Integer>> SUB_TASKS = new ConcurrentHashMap<String, Set<Integer>>();
  private static final ConcurrentMap<String, CacheType> CACHE_TYPES = new ConcurrentHashMap<String, CacheType>();

  @SuppressWarnings("unchecked")
  public static <K, V> ConcurrentMap<K, V> getInsertCache(String cacheId, int subTaskId, Class<K> keyClass, Class<V> valueClass) {
    String finalCacheId = getFinalCacheId(cacheId, subTaskId, CACHE_TYPES.get(cacheId));

    ConcurrentMap<K, V> entry = STORE.get(finalCacheId);
    return entry;
  }

  public static <K, V> void createCache(String cacheId, int subTaskId, CacheType cacheType,
      Class<K> keyClass, Class<V> valueClass) {
    //Set properties for cacheId
    CACHE_TYPES.putIfAbsent(cacheId, cacheType);

    //Get final cache id for cache setup
    String finalCacheId = getFinalCacheId(cacheId, subTaskId, cacheType);

    //Create cache depending on cache type
    if (!STORE.containsKey(finalCacheId)) {
      STORE.putIfAbsent(finalCacheId, new ConcurrentHashMap<K, V>(10000));
    }
    else if (cacheType == CacheType.ISOLATED || cacheType == CacheType.SHARED_READ) {
      throw new RuntimeException("Store already exists: " + finalCacheId);
    }


    //Add subtask id to the list of subtasks belonging to the cacheid
    if (!SUB_TASKS.containsKey(cacheId)) {
      SUB_TASKS.putIfAbsent(cacheId, new HashSet<Integer>());
    }
    Set<Integer> taskIdSet = SUB_TASKS.get(cacheId);
    boolean existed = !taskIdSet.add(subTaskId);
    if (existed) {
      throw new RuntimeException("Subtask already created cache (cache: " + cacheId + ", id: " +subTaskId);
    }
  }

  @SuppressWarnings("unchecked")
  public static <K, V> Iterator<Entry<K, V>> getCachePartition(String cacheId, int subTaskId,
      Class<K> keyClass, Class<V> valueClass) {
    CacheType cacheType = CACHE_TYPES.get(cacheId);
    String finalCacheId = getFinalCacheId(cacheId, subTaskId, cacheType);

    switch(cacheType) {
      case ISOLATED:
      case SHARED_READ:
        return (Iterator<Entry<K, V>>) STORE.get(finalCacheId).entrySet().iterator();
      case SHARED_READ_WRITE:
        //TODO: Test
        int numSubTasks = SUB_TASKS.get(cacheId).size();
        return STORE.get(finalCacheId).getIterators(numSubTasks)[subTaskId];
      default:
        throw new RuntimeException("Unknown cache type " + cacheType.name());
    }
  }

  @SuppressWarnings("unchecked")
  public static <K, V> ConcurrentMap<K, V> getLookupCache(String cacheId, int subTaskId,
      Class<K> keyClass, Class<V> valueClass) {
    CacheType cacheType = CACHE_TYPES.get(cacheId);
    String finalCacheId = getFinalCacheId(cacheId, subTaskId, cacheType);

    switch (cacheType) {
      case ISOLATED:
      case SHARED_READ_WRITE:
        return STORE.get(finalCacheId);
      case SHARED_READ:
        Set<Integer> subTaskList = SUB_TASKS.get(cacheId);

        final ConcurrentMap<K,V>[] stores = new ConcurrentMap[subTaskList.size()];
        int i = 0;
        for (Integer id : subTaskList) {
          finalCacheId = getFinalCacheId(cacheId, id, cacheType);
          stores[i] = STORE.get(finalCacheId);
          i++;
        }
        if (stores.length == 1) {
          return stores[0];
        }

        return new ConcurrentMap<K, V>() {
          final int length = stores.length;

          @Override
          public void clear() {
            throw new UnsupportedOperationException();
          }
          @Override
          public boolean containsKey(Object key) {
            throw new UnsupportedOperationException();
          }
          @Override
          public boolean containsValue(Object value) {
            throw new UnsupportedOperationException();
          }
          @Override
          public Set<java.util.Map.Entry<K, V>> entrySet() {
            throw new UnsupportedOperationException();
          }
          @Override
          public V get(Object key) {
            for (int j = 0; j < length; j++) {
              V value = stores[j].get(key);

              if (value != null) {
                return value;
              }
            }

            return null;
          }
          @Override
          public boolean isEmpty() {
            throw new UnsupportedOperationException();
          }
          @Override
          public Set<K> keySet() {
            throw new UnsupportedOperationException();
          }
          @Override
          public V put(K key, V value) {
            throw new UnsupportedOperationException();
          }
          @Override
          public void putAll(Map<? extends K, ? extends V> m) {
            throw new UnsupportedOperationException();
          }
          @Override
          public V remove(Object key) {
            throw new UnsupportedOperationException();
          }
          @Override
          public int size() {
            throw new UnsupportedOperationException();
          }
          @Override
          public Collection<V> values() {
            throw new UnsupportedOperationException();
          }
          @Override
          public V putIfAbsent(K key, V value) {
            throw new UnsupportedOperationException();
          }
          @Override
          public boolean remove(Object key, Object value) {
            throw new UnsupportedOperationException();
          }
          @Override
          public V replace(K key, V value) {
            throw new UnsupportedOperationException();
          }
          @Override
          public boolean replace(K key, V oldValue, V newValue) {
            throw new UnsupportedOperationException();
          }
        };
      default:
        throw new RuntimeException("Unknown cache type " + cacheType.name());
    }
  }

  private static String getFinalCacheId(String cacheId, int subTaskId, CacheType cacheType) {
    switch (cacheType) {
      case ISOLATED:
      case SHARED_READ:
        return cacheId + subTaskId;
      case SHARED_READ_WRITE:
        return cacheId;
      default:
        throw new RuntimeException("Unknown cache type " + cacheType.name());
    }
  }
}
