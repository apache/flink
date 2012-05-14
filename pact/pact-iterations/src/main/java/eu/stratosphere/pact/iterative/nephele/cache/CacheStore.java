package eu.stratosphere.pact.iterative.nephele.cache;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import eu.stratosphere.pact.iterative.AccessibleConcurrentHashMap;

public class CacheStore {
	public static enum CacheType {
		ISOLATED, SHARED_READ, SHARED_READ_WRITE
	};
	
	@SuppressWarnings("rawtypes")
	private final static ConcurrentMap<String, AccessibleConcurrentHashMap> store = 
			new ConcurrentHashMap<String, AccessibleConcurrentHashMap>();
	
	private final static ConcurrentMap<String, Set<Integer>> subTasks =
			new ConcurrentHashMap<String, Set<Integer>>();
	
	private final static ConcurrentMap<String, CacheType> cacheTypes =
			new ConcurrentHashMap<String, CacheType>();
	
	@SuppressWarnings("unchecked")
	public static <K, V> ConcurrentMap<K, V> getInsertCache(String cacheId, int subTaskId, Class<K> keyClass, Class<V> valueClass) {
		String finalCacheId = getFinalCacheId(cacheId, subTaskId, cacheTypes.get(cacheId));
		
		ConcurrentMap<K, V> entry = store.get(finalCacheId);
		return entry;
	}
	
	public static <K, V> void createCache(String cacheId, int subTaskId, CacheType cacheType,
			Class<K> keyClass, Class<V> valueClass) {
		//Set properties for cacheId
		cacheTypes.putIfAbsent(cacheId, cacheType);
		
		//Get final cache id for cache setup
		String finalCacheId = getFinalCacheId(cacheId, subTaskId, cacheType);
		
		//Create cache depending on cache type
		if(!store.containsKey(finalCacheId)) {
			store.putIfAbsent(finalCacheId, new AccessibleConcurrentHashMap<K, V>(10000));
		} 
		else if(cacheType == CacheType.ISOLATED || cacheType == CacheType.SHARED_READ) {
			throw new RuntimeException("Store already exists: " + finalCacheId);
		}
		
		
		//Add subtask id to the list of subtasks belonging to the cacheid
		if(!subTasks.containsKey(cacheId)) {
			subTasks.putIfAbsent(cacheId, new HashSet<Integer>());
		}
		Set<Integer> taskIdSet = subTasks.get(cacheId);
		boolean existed = !taskIdSet.add(subTaskId);
		if(existed) {
			throw new RuntimeException("Subtask already created cache (cache: " + cacheId + ", id: " +subTaskId);
		}
	}
	
	@SuppressWarnings("unchecked")
	public static <K, V> Iterator<Entry<K, V>> getCachePartition(String cacheId, int subTaskId,
			Class<K> keyClass, Class<V> valueClass) {
		CacheType cacheType = cacheTypes.get(cacheId);
		String finalCacheId = getFinalCacheId(cacheId, subTaskId, cacheType);
		
		switch(cacheType) {
			case ISOLATED:
			case SHARED_READ:
				return (Iterator<Entry<K, V>>) store.get(finalCacheId).entrySet().iterator();
			case SHARED_READ_WRITE:
				//TODO: Test
				int numSubTasks = subTasks.get(cacheId).size();
				return store.get(finalCacheId).getIterators(numSubTasks)[subTaskId];
			default:
				throw new RuntimeException("Unknown cache type " + cacheType.name());
		}
	}
	
	@SuppressWarnings("unchecked")
	public static <K, V> ConcurrentMap<K, V> getLookupCache(String cacheId, int subTaskId,
			Class<K> keyClass, Class<V> valueClass) {
		CacheType cacheType = cacheTypes.get(cacheId);
		String finalCacheId = getFinalCacheId(cacheId, subTaskId, cacheType);
		
		switch (cacheType) {
			case ISOLATED:
			case SHARED_READ_WRITE:
				return store.get(finalCacheId);
			case SHARED_READ:
				Set<Integer> subTaskList = subTasks.get(cacheId);
				
				final ConcurrentMap<K,V>[] stores = new ConcurrentMap[subTaskList.size()];
				int i = 0;
				for (Integer id : subTaskList) {
					finalCacheId = getFinalCacheId(cacheId, id, cacheType);
					stores[i] = store.get(finalCacheId);
					i++;
				}
				if(stores.length == 1) {
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

							if(value != null) {
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
