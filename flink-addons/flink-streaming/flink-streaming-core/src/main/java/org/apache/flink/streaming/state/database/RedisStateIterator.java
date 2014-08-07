/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.flink.streaming.state.database;

import java.util.Iterator;

import redis.clients.jedis.Jedis;

public class RedisStateIterator {
	
	private Iterator<String> iterator;
	private int position;
	private int size;
	private String currentKey;
	private Jedis jedis;
	public RedisStateIterator(Jedis jedis){
		this.jedis = jedis;
		iterator = jedis.keys("*").iterator();
		size = jedis.keys("*").size();
		currentKey = iterator.next();
		position = 0;
	}
	
	public boolean hasNext(){
		return position != size;
	}
	
	public String getNextKey(){
		return currentKey;
	}
	
	public String getNextValue(){
		return jedis.get(currentKey);
	}
	
	public void next(){
		position += 1;
		if (position != size){
			currentKey = iterator.next();
		}
	}
}
