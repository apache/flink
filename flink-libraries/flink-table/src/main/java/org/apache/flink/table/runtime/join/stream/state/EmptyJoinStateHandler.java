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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.dataformat.BaseRow;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The empty implementation for {@link JoinStateHandler}.
 */
public class EmptyJoinStateHandler implements JoinStateHandler {

	@Override
	public void extractCurrentJoinKey(BaseRow row) throws Exception {

	}

	@Override
	public BaseRow getCurrentJoinKey() {
		return null;
	}

	@Override
	public void extractCurrentPrimaryKey(BaseRow row) {

	}

	@Override
	public BaseRow getCurrentPrimaryKey() {
		return null;
	}

	@Override
	public long add(BaseRow row, long expireTime) {
		return 0;
	}

	@Override
	public long retract(BaseRow row) {
		return 0;
	}

	@Override
	public Iterator<Tuple3<BaseRow, Long, Long>> getRecords(BaseRow key) {
		return Collections.EMPTY_LIST.iterator();
	}

	@Override
	public Iterator<Tuple3<BaseRow, Long, Long>> getRecordsFromCache(BaseRow key) {
		return getRecords(key);
	}

	@Override
	public boolean contains(BaseRow key, BaseRow row) {
		return false;
	}

	@Override
	public void update(BaseRow key, BaseRow row, long count, long expireTime) {

	}

	@Override
	public void remove(BaseRow joinKey) {
	}

	@Override
	public void batchGet(Collection<? extends BaseRow> keys) {
	}

	@Override
	public long[] batchUpdate(BaseRow key, List<Tuple2<BaseRow, Long>> rows, long expireTime) {
		return null;
	}

	@Override
	public void setCurrentJoinKey(BaseRow row) {
	}

	@Override
	public void putAll(Map<BaseRow, BaseRow> putMap) {
	}

	@Override
	public void removeAll(Set<BaseRow> keys) {
	}
}
