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

package org.apache.flink.formats.parquet.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.io.api.RecordMaterializer.RecordMaterializationException;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.CheckReturnValue;
import javax.annotation.meta.When;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.parquet.Preconditions.checkNotNull;

/**
 *
 * @param <T>
 */
public class ParquetRecordReader<T> {
	private static final Logger LOG = LoggerFactory.getLogger(ParquetRecordReader.class);

	private ColumnIOFactory columnIOFactory;
	private Filter filter;

	private MessageType readSchema;
	private MessageType fileSchema;
	private ReadSupport<T> readSupport;

	private RecordMaterializer<T> recordMaterializer;
	private T currentValue;
	private long total;
	private long current = 0;
	private int currentBlock = -1;
	private ParquetFileReader reader;
	private RecordReader<T> recordReader;
	private boolean strictTypeChecking = true;
	private long totalCountLoadedSoFar = 0;

	public ParquetRecordReader(ReadSupport<T> readSupport, MessageType readSchema, Filter filter) {
		this.readSupport = readSupport;
		this.readSchema = readSchema;
		this.filter = checkNotNull(filter, "filter");
	}

	public ParquetRecordReader(ReadSupport<T> readSupport, MessageType readSchema) {
		this(readSupport, readSchema, FilterCompat.NOOP);
	}

	public void initialize(ParquetFileReader reader, Configuration configuration) {
		this.reader = reader;
		FileMetaData parquetFileMetadata = reader.getFooter().getFileMetaData();
		this.fileSchema = parquetFileMetadata.getSchema();
		Map<String, String> fileMetadata = parquetFileMetadata.getKeyValueMetaData();
		ReadSupport.ReadContext readContext = readSupport.init(new InitContext(
			configuration, toSetMultiMap(fileMetadata), readSchema));

		this.columnIOFactory = new ColumnIOFactory(parquetFileMetadata.getCreatedBy());
		this.readSchema = readContext.getRequestedSchema();
		this.recordMaterializer = readSupport.prepareForRead(
			configuration, fileMetadata, readSchema, readContext);
		this.total = reader.getRecordCount();
		reader.setRequestedSchema(readSchema);
	}

	private void checkRead() throws IOException {
		if (current == totalCountLoadedSoFar) {
			PageReadStore pages = reader.readNextRowGroup();
			recordReader = createRecordReader(pages);
			totalCountLoadedSoFar += pages.getRowCount();
			currentBlock++;
		}
	}

	public void close() throws  IOException {
		if (reader != null) {
			reader.close();
		}
	}

	@CheckReturnValue(when = When.NEVER)
	public T nextRecord() {
		return currentValue;
	}

	public void seek(long syncedBlock) throws IOException {
		PageReadStore pages = null;
		while (syncedBlock > 0) {
			pages = reader.readNextRowGroup();
		}

		recordReader = createRecordReader(pages);
	}

	private RecordReader<T> createRecordReader(PageReadStore pages) throws IOException {
		if (pages == null) {
			throw new IOException("Expecting more rows but reached last block. Read " + current + " out of " + total);
		}
		MessageColumnIO columnIO = columnIOFactory.getColumnIO(readSchema, fileSchema, strictTypeChecking);
		// TODO (hpeter) enable filter later
		RecordReader<T> recordReader = columnIO.getRecordReader(pages, recordMaterializer, FilterCompat.NOOP);
		return recordReader;
	}

	public long getCurrentBlock() {
		return currentBlock;
	}

	public boolean reachEnd() {
		return current >= total;
	}

	public boolean hasNextRecord() throws IOException {
		boolean recordFound = false;

		while (!recordFound) {
			// no more records left
			if (current >= total) {
				return false;
			}

			try {
				checkRead();
				current++;
				try {
					currentValue = recordReader.read();
				} catch (RecordMaterializationException e) {
					LOG.debug("skipping a corrupt record");
					continue;
				}

				if (recordReader.shouldSkipCurrentRecord()) {
					LOG.debug("skipping record");
				}

				if (currentValue == null) {
					current = totalCountLoadedSoFar;
					LOG.debug("filtered record reader reached end of block");
					continue;
				}

				recordFound = true;
				LOG.debug("read value: {}", currentValue);
			} catch (RuntimeException e) {
				LOG.error(String.format("Can not read value at %d in block %d in file %s",
					current, currentBlock, reader.getFile()), e);
			}
		}

		return true;
	}

	private static <K, V> Map<K, Set<V>> toSetMultiMap(Map<K, V> map) {
		Map<K, Set<V>> setMultiMap = new HashMap<K, Set<V>>();
		for (Map.Entry<K, V> entry : map.entrySet()) {
			Set<V> set = new HashSet<V>();
			set.add(entry.getValue());
			setMultiMap.put(entry.getKey(), Collections.unmodifiableSet(set));
		}
		return Collections.unmodifiableMap(setMultiMap);
	}
}
