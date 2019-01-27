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

package org.apache.flink.table.runtime.join.stream.state.match;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.dataformat.BaseRow;

import java.util.Map;
import java.util.Set;

/**
 * Wrap State which record the match times of join.
 */
@Internal
public interface JoinMatchStateHandler {
	/**
	 * match handler type.
	 */
	enum Type {
		EMPTY_MATCH, //ignore the match times.
		ONLY_EQUALITY_CONDITION_EMPTY_MATCH, //ignore the match times if the join info only contain equality
		JOIN_KEY_CONTAIN_PRIMARY_KEY_MATCH, //hold the match times based value state.
		JOIN_KEY_NOT_CONTAIN_PRIMARY_KEY_MATCH, //hold the match times based value state.
		WITHOUT_PRIMARY_KEY_MATCH //hold the match times based map state.
	}

	/**
	 * extract and hold the match times for the given record.
	 * @param joinKey the join key of the record
	 * @param row the record
	 */
	void extractCurrentRowMatchJoinCount(BaseRow joinKey, BaseRow row, long possibleJoinCnt);

	/**
	 * get the match times which is hold.
	 *
	 * @return the match times
	 */
	long getCurrentRowMatchJoinCnt();

	/**
	 * reset the match times into state under the hold key.
	 *
	 * @param joinCnt the match times
	 */
	void resetCurrentRowMatchJoinCnt(long joinCnt);

	/**
	 * update the match times into state under the given record. If the key
	 * is already associated with a value, the value will be replaced with the given value.
	 *
	 * @param joinKey the join key of the record
	 * @param baseRow the record
	 * @param joinCnt the match times
	 */
	void updateRowMatchJoinCnt(BaseRow joinKey, BaseRow baseRow, long joinCnt);

	/**
	 * update the match times into state under the given record. If the keys
	 * is already associated with a value, the value will be accumulative with the
	 * given value .
	 *
	 * @param joinKey the join key of the record
	 * @param baseRow the record
	 * @param joinCnt the match times
	 */
	void addRowMatchJoinCnt(BaseRow joinKey, BaseRow baseRow, long joinCnt);

	/**
	 * remove the record from the state.
	 *
	 * @param joinKey the join key of the record
	 * @param baseRow the record
	 */
	void remove(BaseRow joinKey, BaseRow baseRow);

	/**
	 * remove all records of the joinKey from the state.
	 *
	 * @param joinKey the join key of the record
	 */
	void remove(BaseRow joinKey);

	/**
	 * Batch remove keys for the joinKey.
	 *
	 * @param joinKey the join key of the records
	 * @param keys    the keys to be removed
	 */
	void removeAll(BaseRow joinKey, Set<BaseRow> keys);

	/**
	 * Batch update.
	 *
	 * @param joinKey the join key of the records
	 * @param kvs     the key and value map
	 */
	void addAll(BaseRow joinKey, Map<BaseRow, Long> kvs);
}
