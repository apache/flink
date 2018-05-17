/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.util.FlinkRuntimeException;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.RocksIteratorInterface;

import javax.annotation.Nonnull;
import java.io.Closeable;

/**
 * This is a wrapper that wraps {@link RocksIterator} to check status there for all the methods mentioned
 * in the wiki documentation: Seek(), Next(), SeekToFirst(), SeekToLast(), SeekForPrev(), and Prev() to be
 * on the safe side, because the iterator may pass the blocks or files it had difficulties in reading (because
 * of IO error, data corruption or other issues) and continue with the next available keys. status() may not OK,
 * even if Valid()=true. more information can be found in https://github.com/facebook/rocksdb/wiki/Iterator#error-handling
 */
public class RocksIteratorWrapper implements RocksIteratorInterface, Closeable {

	private RocksIterator iterator;

	public RocksIteratorWrapper(@Nonnull RocksIterator iterator) {
		this.iterator = iterator;
	}

	@Override
	public boolean isValid() {
		return this.iterator.isValid();
	}

	@Override
	public void seekToFirst() {
		iterator.seekToFirst();
		status();
	}

	@Override
	public void seekToLast() {
		iterator.seekToFirst();
		status();
	}

	@Override
	public void seek(byte[] target) {
		iterator.seek(target);
		status();
	}

	@Override
	public void next() {
		iterator.next();
		status();
	}

	@Override
	public void prev() {
		iterator.prev();
		status();
	}

	@Override
	public void status() {
		try {
			iterator.status();
		} catch (RocksDBException ex) {
			throw new FlinkRuntimeException("Internal exception found in RocksDB", ex);
		}
	}

	public byte[] key() {
		return iterator.key();
	}

	public byte[] value() {
		return iterator.value();
	}

	@Override
	public void close() {
		iterator.close();
	}
}
