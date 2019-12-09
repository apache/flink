/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.util;

import org.apache.flink.shaded.guava18.com.google.common.collect.Iterators;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JavaType;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.type.TypeFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Json scalar function util.
 */
public class JsonUtils {
	public static final Logger LOG = LoggerFactory.getLogger(JsonUtils.class);
	public final Pattern patternKey = Pattern.compile("^([a-zA-Z0-9_\\-\\:\\s]+).*");
	public final Pattern patternIndex = Pattern.compile("\\[([0-9]+|\\*)\\]");

	public static final JsonFactory JSON_FACTORY = new JsonFactory();

	static {
		// Allows for unescaped ASCII control characters in JSON values
		JSON_FACTORY.enable(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS);
	}

	public static final ObjectMapper MAPPER = new ObjectMapper(JSON_FACTORY);
	public static final JavaType MAP_TYPE = TypeFactory.defaultInstance().constructMapType(Map.class, Object.class, Object.class);
	public static final JavaType LIST_TYPE = TypeFactory.defaultInstance().constructRawCollectionType(List.class);

	/**
	 *An LRU cache using a linked hash map.
	 */
	public static class HashCache<K, V> extends LinkedHashMap<K, V> {

		private static final int CACHE_SIZE = 16;
		private static final int INIT_SIZE = 32;
		private static final float LOAD_FACTOR = 0.6f;

		HashCache() {
			super(INIT_SIZE, LOAD_FACTOR);
		}

		private static final long serialVersionUID = 1;

		@Override
		protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
			return size() > CACHE_SIZE;
		}

	}

	/**
	 *An ThreadLocal cache using a linked hash map.
	 */
	public static class ThreadLocalHashCache<K, V> {
		private ThreadLocal<HashCache<K, V>> cache = new ThreadLocal<>();

		public V get(K key) {
			HashCache<K, V> m = cache.get();
			if (m == null) {
				m = new HashCache<>();
				cache.set(m);
			}
			return m.get(key);
		}

		public V put(K key, V value) {
			HashCache<K, V> m = cache.get();
			if (m == null) {
				m = new HashCache<>();
				cache.set(m);
			}
			return m.put(key, value);
		}

		public void remove() {
			cache.remove();
		}
	}

	public static ThreadLocalHashCache<String, Object> extractObjectCache = new ThreadLocalHashCache<String, Object>();
	public static ThreadLocalHashCache<String, String[]> pathExprCache = new ThreadLocalHashCache<String, String[]>();
	public static ThreadLocalHashCache<String, ArrayList<String>> indexListCache =
			new ThreadLocalHashCache<String, ArrayList<String>>();
	public static ThreadLocalHashCache<String, String> mKeyGroup1Cache = new ThreadLocalHashCache<String, String>();
	public static ThreadLocalHashCache<String, Boolean> mKeyMatchesCache = new ThreadLocalHashCache<String, Boolean>();

	private static ThreadLocal<JsonUtils> instance = new ThreadLocal<JsonUtils>();

	public static void remove(){
		instance.remove();
	}

	public static JsonUtils getInstance() {
		if (null == instance.get()) {
			instance.set(new JsonUtils());
		}
		return instance.get();
	}

	public String getJsonObject(String jsonString, String pathString) {

		if (jsonString == null || jsonString.isEmpty() || pathString == null
				|| pathString.isEmpty() || pathString.charAt(0) != '$') {
			LOG.error("jsonString is null or empty, or path is null or empty, or path is not start with '$'! " +
					"jsonString: " + jsonString + ", path: " + pathString);
			return null;
		}

		String result = new String();
		int pathExprStart = 1;
		boolean isRootArray = false;

		if (pathString.length() > 1) {
			if (pathString.charAt(1) == '[') {
				pathExprStart = 0;
				isRootArray = true;
			} else if (pathString.charAt(1) == '.') {
				isRootArray = pathString.length() > 2 && pathString.charAt(2) == '[';
			} else {
				LOG.error("path String illegal! path String: " + pathString);
				return null;
			}
		}

		// Cache pathExpr
		String[] pathExpr = pathExprCache.get(pathString);
		if (pathExpr == null) {
			pathExpr = pathString.split("\\.", -1);
			pathExprCache.put(pathString, pathExpr);
		}

		// Cache extractObject
		Object extractObject = extractObjectCache.get(jsonString);
		if (extractObject == null) {
			JavaType javaType = isRootArray ? LIST_TYPE : MAP_TYPE;
			try {
				extractObject = MAPPER.readValue(jsonString, javaType);
			} catch (Exception e) {
				LOG.error(
						"Exception when read json value with type :" + javaType.toString() +
						", and json string: " + jsonString, e);
				return null;
			}
			extractObjectCache.put(jsonString, extractObject);
		}
		for (int i = pathExprStart; i < pathExpr.length; i++) {
			if (extractObject == null) {
				LOG.error("path look up fail at: " + pathExpr[i - 1 >= 0 ? i - 1 : 0] +
						", pathString: " + pathString +
						"json: " + jsonString);
				return null;
			}
			extractObject = extract(extractObject, pathExpr[i], i == pathExprStart && isRootArray);
		}
		if (extractObject instanceof Map || extractObject instanceof List) {
			try {
				result = MAPPER.writeValueAsString(extractObject);
			} catch (Exception e) {
				LOG.error(
						"Exception when MAPPER.writeValueAsString :" +
								extractObject.toString(), e);
				return null;
			}
		} else if (extractObject != null) {
			result = extractObject.toString();
		} else {
			LOG.error("path look up fail at: " + (pathExpr.length - 1 >= 0 ? pathExpr[pathExpr.length - 1] : null) +
					", pathString: " + pathString +
					"json: " + jsonString);
			return null;
		}
		return result;
	}

	public String[] getJsonObjectsWithoutDollar(String jsonString, String[] pathStrings) {
		if (jsonString == null || jsonString.isEmpty() || pathStrings == null
				|| pathStrings.length == 0) {
			LOG.error("jsonString is null or empty, or path is null or empty! " +
					"jsonString: " + jsonString);
			return new String[0];
		}

		int pathExprStart = 1;
		boolean isRootArray = false;

		Object rootExtractObject = extractObjectCache.get(jsonString);
		if (rootExtractObject == null) {
			JavaType javaType = isRootArray ? LIST_TYPE : MAP_TYPE;
			try {
				rootExtractObject = MAPPER.readValue(jsonString, javaType);
			} catch (Exception e) {
				LOG.error(
						"Exception when read json value with type :" + javaType.toString() +
								", and json string: " + jsonString, e);
				return new String[0];
			}
			extractObjectCache.put(jsonString, rootExtractObject);
		}

		String[] result = new String[pathStrings.length];
		for (int i = 0; i < pathStrings.length; i++) {
			String pathString = "$." + pathStrings[i];
			if (pathString == null || pathString.length() == 0){
				result[i] = null;
				LOG.error(i + "th path String is null or empty! " +
						"pathString: " + pathString);
				continue;
			}
			if (pathString.length() > 1) {
				if (pathString.charAt(1) == '[') {
					pathExprStart = 0;
					isRootArray = true;
				} else if (pathString.charAt(1) == '.') {
					isRootArray = pathString.length() > 2 && pathString.charAt(2) == '[';
				} else {
					result[i] = null;
					LOG.error(i + "th path String illegal! path String: " + pathString);
					continue;
				}
			}

			// Cache pathExpr
			String[] pathExpr = pathExprCache.get(pathString);
			if (pathExpr == null) {
				pathExpr = pathString.split("\\.", -1);
				pathExprCache.put(pathString, pathExpr);
			}

			// Cache extractObject
			Object extractObject = rootExtractObject;
			if (extractObject == null) {
				JavaType javaType = isRootArray ? LIST_TYPE : MAP_TYPE;
				try {
					extractObject = MAPPER.readValue(jsonString, javaType);
				} catch (Exception e) {
					LOG.error(
							"Exception when read json value with type :" + javaType.toString() +
									", and json string: " + jsonString, e);
					result[i] = null;
					continue;
				}
				extractObjectCache.put(jsonString, extractObject);
			}
			for (int j = pathExprStart; j < pathExpr.length; j++) {
				if (extractObject == null) {
					result[i] = null;
					LOG.error(i + "th path look up fail at: " + pathExpr[j - 1 >= 0 ? j - 1 : 0] +
							", pathString: " + pathString +
							"json: " + jsonString);
					continue;
				}
				extractObject = extract(extractObject, pathExpr[j], j == pathExprStart && isRootArray);
			}
			if (extractObject instanceof Map || extractObject instanceof List) {
				try {
					result[i] = MAPPER.writeValueAsString(extractObject);
				} catch (Exception e) {
					LOG.error(
							"Exception when MAPPER.writeValueAsString :" +
									extractObject.toString(), e);
					result[i] = null;
					continue;
				}
			} else if (extractObject != null) {
				result[i] = extractObject.toString();
			} else {
				result[i] = null;
				LOG.error(i + "th path look up fail at: " +
						(pathExpr.length - 1 >= 0 ? pathExpr[pathExpr.length - 1] : null) +
						", pathString: " + pathString +
						"json: " + jsonString);
				continue;
			}
		}
		return result;
	}

	protected Object extract(Object json, String path, boolean skipMapProc) {
		// skip MAP processing for the first path element if root is array
		if (!skipMapProc) {
			// Cache patternkey.matcher(path).matches()
			Matcher mKey = null;
			Boolean mKeyMatches = mKeyMatchesCache.get(path);
			if (mKeyMatches == null) {
				mKey = patternKey.matcher(path);
				mKeyMatches = mKey.matches() ? Boolean.TRUE : Boolean.FALSE;
				mKeyMatchesCache.put(path, mKeyMatches);
			}
			if (!mKeyMatches.booleanValue()) {
				return null;
			}

			// Cache mkey.group(1)
			String mKeyGroup1 = mKeyGroup1Cache.get(path);
			if (mKeyGroup1 == null) {
				if (mKey == null) {
					mKey = patternKey.matcher(path);
					mKeyMatches = mKey.matches() ? Boolean.TRUE : Boolean.FALSE;
					mKeyMatchesCache.put(path, mKeyMatches);
					if (!mKeyMatches.booleanValue()) {
						return null;
					}
				}
				mKeyGroup1 = mKey.group(1);
				mKeyGroup1Cache.put(path, mKeyGroup1);
			}
			json = extractJsonWithkey(json, mKeyGroup1);
		}
		// Cache indexList
		ArrayList<String> indexList = indexListCache.get(path);
		if (indexList == null) {
			Matcher mIndex = patternIndex.matcher(path);
			indexList = new ArrayList<String>();
			while (mIndex.find()) {
				indexList.add(mIndex.group(1));
			}
			indexListCache.put(path, indexList);
		}

		if (indexList.size() > 0) {
			json = extractJsonWithIndex(json, indexList);
		}

		return json;
	}

	private AddingList jsonList = new AddingList();

	private static class AddingList extends ArrayList<Object> {
		@Override
		public java.util.Iterator<Object> iterator() {
			return Iterators.forArray(toArray());
		}

		@Override
		public void removeRange(int fromIndex, int toIndex) {
			super.removeRange(fromIndex, toIndex);
		}
	}

	protected Object extractJsonWithIndex(Object json, ArrayList<String> indexList) {

		jsonList.clear();
		jsonList.add(json);
		AddingList tempJsonList = new AddingList();
		for (String index : indexList) {
			int targets = jsonList.size();
			if (index.equalsIgnoreCase("*")) {
				for (Object array : jsonList) {
					if (array instanceof List) {
						for (int j = 0; j < ((List<Object>) array).size(); j++) {
							jsonList.add(((List<Object>) array).get(j));
						}
					}
				}
			} else {
				for (Object array : jsonList) {
					int indexValue = Integer.parseInt(index);
					if (!(array instanceof List)) {
						continue;
					}
					List<Object> list = (List<Object>) array;
					if (indexValue >= list.size()) {
						continue;
					}
					tempJsonList.add(list.get(indexValue));
				}
				jsonList.addAll(tempJsonList);
			}
			if (jsonList.size() == targets) {
				return null;
			}
			jsonList.removeRange(0, targets);
		}
		if (jsonList.isEmpty()) {
			return null;
		}
		return (jsonList.size() > 1) ? new ArrayList<Object>(jsonList) : jsonList.get(0);
	}

	protected Object extractJsonWithkey(Object json, String path) {
		if (json instanceof List) {
			List<Object> jsonArray = new ArrayList<Object>();
			for (int i = 0; i < ((List<Object>) json).size(); i++) {
				Object jsonElem = ((List<Object>) json).get(i);
				Object jsonObj = null;
				if (jsonElem instanceof Map) {
					jsonObj = ((Map<String, Object>) jsonElem).get(path);
				} else {
					continue;
				}
				if (jsonObj instanceof List) {
					for (int j = 0; j < ((List<Object>) jsonObj).size(); j++) {
						jsonArray.add(((List<Object>) jsonObj).get(j));
					}
				} else if (jsonObj != null) {
					jsonArray.add(jsonObj);
				}
			}
			return (jsonArray.size() == 0) ? null : jsonArray;
		} else if (json instanceof Map) {
			return ((Map<String, Object>) json).get(path);
		} else {
			return null;
		}
	}
}
