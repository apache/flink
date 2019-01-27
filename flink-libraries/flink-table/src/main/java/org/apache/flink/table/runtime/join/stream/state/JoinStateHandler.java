/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copysecond ownership.
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
 */

package org.apache.flink.table.runtime.join.stream.state;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.dataformat.BaseRow;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Wrap KeyedState for Join.
 */
@Internal
public interface JoinStateHandler {

	/**
	 * join handler type.
	 */
	enum Type {
		EMPTY, //do nothing
		JOIN_KEY_CONTAIN_PRIMARY_KEY, //join keys contain pk.
		JOIN_KEY_NOT_CONTAIN_PRIMARY_KEY, //the record take primary key, but the pk isn't contain by the join keys.
		WITHOUT_PRIMARY_KEY, //the record don't take primary key.
		COUNT_KEY_SIZE //the record don't take primary key.
	}

	/**
	 * extract and hold the joinKey from the record.
	 *
	 * @param row the record
	 */
	void extractCurrentJoinKey(BaseRow row) throws Exception;

	/**
	 * Set current join key.
	 * @param row the input join key
	 */
	void setCurrentJoinKey(BaseRow row);

	/**
	 * get the joinKey which is hold.
	 *
	 * @return the joinKey
	 */
	BaseRow getCurrentJoinKey();

	/**
	 * extract and primaryKey from the record.
	 *
	 * @param row the record
	 */
	void extractCurrentPrimaryKey(BaseRow row);

	/**
	 * get the primaryKey which is hold.
	 *
	 * @return the primaryKey
	 */
	BaseRow getCurrentPrimaryKey();

	/**
	 * put the record into state under the hold key. If the key is
	 * already associated with a value, the value will be accumulative.
	 *
	 * @param row the record
	 * @param expireTime the expire time
	 * @return the number of replication of the record
	 */
	long add(BaseRow row, long expireTime);

	/**
	 * retract the record from the state under the hold key.
	 *
	 * @param row the record
	 * @return the number of replication of the record
	 */
	long retract(BaseRow row);

	/**
	 * Returns an iterator over the elements under the given key. The order in
	 * which the elements are iterated is not specified. The return is A tuple
	 * with 3 fields. Field 0 of the tuple means the real record, Field 1 of the
	 * tuple means the number of replication of the record, Field 2 of the tuple
	 * means the expire time of the record.
	 *
	 * @param key The key under which the elements are to be iterated.
	 * @return An iterator over the elements under the given key.
	 */
	Iterator<Tuple3<BaseRow, Long, Long>> getRecords(BaseRow key);

	/**
	 * Returns an iterator over the elements under the given key from cache.
	 *
	 * @param key The key under which the elements are to be iterated.
	 * @return An iterator over the elements under the given key.
	 */
	Iterator<Tuple3<BaseRow, Long, Long>> getRecordsFromCache(BaseRow key);

	/**
	 * Returns true if the state contains a pair whose key and row is equal to the given
	 * objects.
	 *
	 * @param key The key whose presence in the state to be tested.
	 * @param row The row whose presence in the state to be tested.
	 * @return True if the state contains the pair for the given key and the row.
	 */
	boolean contains(BaseRow key, BaseRow row);

	/**
	 * Associates the given count with the given key and the given row in the state. If the keys
	 * is already associated with a value, the value will be replaced with the
	 * given value .
	 *
	 * @param key The key with which the given value is to be associated.
	 * @param row The record to with which the given value is to be associated.
	 * @param count The value for the given key and the row, means the the number of replication.
	 * @param expireTime the expire time
	 */
	void update(BaseRow key, BaseRow row, long count, long expireTime);

	/**
	 * remove all records of the joinKey from the state.
	 * @param joinKey the join key of the record
	 */
	void remove(BaseRow joinKey);

	/**
	 * Batch update rows for the specified key.
	 *
	 * @param key          the join key
	 * @param rows         the input rows
	 * @param expireTime   the expire time
	 * @return             the udpate status, return -1 if row cnt is from n to 0, return 1 if
	 * row cnt is from 0 to n, return 0 if row cnt is from n to m.
	 */
	long[] batchUpdate(BaseRow key, List<Tuple2<BaseRow, Long>> rows, long expireTime);

	/**
	 * Batch get different key's data, only used for valued state.
	 *
	 * @param keys different join keys
	 */
	void batchGet(Collection<? extends BaseRow> keys);

	/**
	 * Batch put different key's data, only used for valued state.
	 *
	 * @param putMap
	 */
	void putAll(Map<BaseRow, BaseRow> putMap);

	/**
	 * Batch delete different key's data, only used for valued state.
	 *
	 * @param keys The input keys
	 */
	void removeAll(Set<BaseRow> keys);
}
