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
import java.util.Iterator;
import java.util.Map.Entry;

import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.streaming.index.IndexPair;

public class LogTableStateIterator<K, V> implements TableStateIterator<K ,V>{

	private Iterator<Entry<K, IndexPair>> iterator;
	private HashMap<Integer, ArrayList<V>> blockList;
	public LogTableStateIterator(Iterator<Entry<K, IndexPair>> iter, HashMap<Integer, ArrayList<V>> blocks){
		iterator=iter;
		blockList=blocks;
	}
	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Tuple2<K, V> next() {
		// TODO Auto-generated method stub
		return null;
	}
	
}
