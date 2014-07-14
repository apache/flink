/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.streaming.state;

import java.util.ArrayList;
import java.util.HashMap;

import eu.stratosphere.streaming.index.IndexPair;

/**
 * The log-structured key value store thats accept any modification operation by
 * appending the value to the end of the state.
 */
public class LogTableState<K, V> implements TableState<K, V> {

	private HashMap<K, IndexPair> hashMap = new HashMap<K, IndexPair>();
	private HashMap<Integer, ArrayList<V>> blockList = new HashMap<Integer, ArrayList<V>>();
	private final int perBlockEntryCount = 1000;
	private IndexPair nextInsertPos = new IndexPair(-1, -1);

	public LogTableState() {
		blockList.put(0, new ArrayList<V>());
		nextInsertPos.setIndexPair(0, 0);
	}

	@Override
	public void put(K key, V value) {
		// TODO Auto-generated method stub
		if (nextInsertPos.entryId == perBlockEntryCount) {
			blockList.put(nextInsertPos.blockId + 1, new ArrayList<V>());
			nextInsertPos.IncrementBlock();
		}
		blockList.get(nextInsertPos.blockId).add(value);
		hashMap.put(key, new IndexPair(nextInsertPos));
		nextInsertPos.entryId += 1;
	}

	@Override
	public V get(K key) {
		// TODO Auto-generated method stub
		IndexPair index = hashMap.get(key);
		if (index == null) {
			return null;
		} else {
			return blockList.get(index.blockId).get(index.entryId);
		}
	}

	@Override
	public void delete(K key) {
		// TODO Auto-generated method stub
		hashMap.remove(key);
	}

	@Override
	public boolean containsKey(K key) {
		// TODO Auto-generated method stub
		return hashMap.containsKey(key);
	}

	@Override
	public String serialize() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void deserialize(String str) {
		// TODO Auto-generated method stub

	}

	@Override
	public TableStateIterator<K, V> getIterator() {
		// TODO Auto-generated method stub
		return new LogTableStateIterator<K, V>(hashMap.entrySet().iterator(), blockList);
	}

}
