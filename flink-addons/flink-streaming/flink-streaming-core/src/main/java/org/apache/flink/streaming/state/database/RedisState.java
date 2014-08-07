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

import redis.clients.jedis.Jedis;

//this is the redis-supported state. To use this state, the users are required to boot their redis server first.
public class RedisState {
	
	private Jedis jedis;
	
	public RedisState(){
		jedis = new Jedis("localhost");
	}
	
	public void close(){
		jedis.close();
	}
	
	public void setTuple(String key, String value){
		jedis.set(key, value);
	}
	
	public String getTuple(String key){
		return jedis.get(key);
	}
	
	public void deleteTuple(String key){
		jedis.del(key);
	}
	
	public RedisStateIterator getIterator(){
		return new RedisStateIterator(jedis);
	}

}
