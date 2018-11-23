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
import org.apache.parquet.hadoop.metadata.BlockMetaData;
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
import java.util.List;
import java.util.Map;
import java.util.Set;


import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Customized {@link org.apache.parquet.hadoop.ParquetRecordReader} that support start read from particular position.
 */
public class ParquetRecordReader<T> {
	private static final Logger LOG = LoggerFactory.getLogger(ParquetRecordReader.class);

	private ColumnIOFactory columnIOFactory;
	private Filter filter;
	private MessageType readSchema;
	private MessageType fileSchema;
	private ReadSupport<T> readSupport;

	// Parquet Materializer convert record to T
	private RecordMaterializer<T> recordMaterializer;
	private T currentValue;
	private long numTotalRows;
	private long numReturnedRows = 0;
	private int currentBlock = -1;
	private ParquetFileReader reader;
	private RecordReader<T> recordReader;
	private boolean strictTypeChecking = true;
	private boolean skipCorruptedRecord = true;
	private boolean fetched = false;
	private long numRowsUpToPreviousBlock = 0;
	private long numRowsUpToCurrentBlock = 0;

	public ParquetRecordReader(ReadSupport<T> readSupport, MessageType readSchema, Filter filter) {
		this.filter = checkNotNull(filter, "readSupport");
		this.readSupport = checkNotNull(readSupport, "readSchema");
		this.readSchema = checkNotNull(readSchema, "filter");
	}

	public ParquetRecordReader(ReadSupport<T> readSupport, MessageType readSchema) {
		this(readSupport, readSchema, FilterCompat.NOOP);
	}

	public void setSkipCorruptedRecord(boolean skipCorruptedRecord) {
		this.skipCorruptedRecord = skipCorruptedRecord;
	}

	public void initialize(ParquetFileReader reader, Configuration configuration) {
		this.reader = reader;
		FileMetaData parquetFileMetadata = reader.getFooter().getFileMetaData();
		// real schema of parquet file
		this.fileSchema = parquetFileMetadata.getSchema();
		Map<String, String> fileMetadata = parquetFileMetadata.getKeyValueMetaData();
		ReadSupport.ReadContext readContext = readSupport.init(new InitContext(
			configuration, toSetMultiMap(fileMetadata), readSchema));

		this.columnIOFactory = new ColumnIOFactory(parquetFileMetadata.getCreatedBy());
		this.recordMaterializer = readSupport.prepareForRead(
			configuration, fileMetadata, readSchema, readContext);
		this.numTotalRows = reader.getRecordCount();
	}

	private RecordReader<T> createRecordReader(PageReadStore pages) throws IOException {
		if (pages == null) {
			throw new IOException("Expecting more rows but reached last block. Read "
				+ numReturnedRows + " out of " + numTotalRows);
		}
		MessageColumnIO columnIO = columnIOFactory.getColumnIO(readSchema, fileSchema, strictTypeChecking);
		RecordReader<T> recordReader = columnIO.getRecordReader(pages, recordMaterializer, filter);
		return recordReader;
	}

	public void seek(long syncedBlock, long recordInBlock) throws IOException {
		List<BlockMetaData> blockMetaData = reader.getRowGroups();
		currentBlock = 0;
		while (currentBlock < syncedBlock) {
			currentBlock++;
			reader.skipNextRowGroup();
			numRowsUpToPreviousBlock = numRowsUpToCurrentBlock;
			numRowsUpToCurrentBlock += blockMetaData.get(currentBlock).getRowCount();
		}

		PageReadStore pages = reader.readNextRowGroup();
		recordReader = createRecordReader(pages);

		for (int i = 0; i < recordInBlock; i++) {
			// skip the record already processed
			if (hasNextRecord()) {
				nextRecord();
			}
		}

	}

	public boolean reachEnd() {
		return numReturnedRows >= numTotalRows;
	}

	public boolean hasNextRecord() throws IOException {
		boolean recordFound = false;
		if (currentValue != null && !fetched) {
			return true;
		}

		while (!recordFound) {
			// no more records left
			if (numReturnedRows >= numTotalRows) {
				return false;
			}

			try {
				if (numReturnedRows == numRowsUpToCurrentBlock) {
					PageReadStore pages = reader.readNextRowGroup();
					recordReader = createRecordReader(pages);
					numRowsUpToPreviousBlock = numRowsUpToCurrentBlock;
					numRowsUpToCurrentBlock += pages.getRowCount();
					currentBlock++;
				}

				numReturnedRows++;
				try {
					currentValue = recordReader.read();
				} catch (RecordMaterializationException e) {
					String errorMessage = String.format("skipping a corrupt record in block number [%d] record"
						+ "number [%s] of file %s", currentBlock,
						numReturnedRows - numRowsUpToPreviousBlock, reader.getFile());

					if (!skipCorruptedRecord) {
						LOG.error(errorMessage);
						throw e;
					} else {
						LOG.warn(errorMessage);
					}
					continue;
				}

				if (currentValue == null) {
					numReturnedRows = numRowsUpToCurrentBlock;
					LOG.debug("filtered record reader reached end of block");
					continue;
				}

				recordFound = true;
				LOG.debug("read value: {}", currentValue);
			} catch (RecordMaterializationException e) {
				LOG.error(String.format("Can not read value at %d in block %d in file %s",
					numReturnedRows - numRowsUpToPreviousBlock, currentBlock, reader.getFile()), e);
				if (!skipCorruptedRecord) {
					throw e;
				}
				return false;
			}
		}

		return true;
	}

	@CheckReturnValue(when = When.NEVER)
	public T nextRecord() {
		fetched = true;
		return currentValue;
	}

	public long getCurrentBlock() {
		return currentBlock;
	}

	public long getRecordInCurrentBlock() {
		if (currentBlock == 0) {
			return numReturnedRows;
		} else {
			return numReturnedRows - numRowsUpToPreviousBlock;
		}
	}

	public void close() throws  IOException {
		if (reader != null) {
			reader.close();
		}
	}

	private static <K, V> Map<K, Set<V>> toSetMultiMap(Map<K, V> map) {
		Map<K, Set<V>> setMultiMap = new HashMap<>();
		for (Map.Entry<K, V> entry : map.entrySet()) {
			Set<V> set = Collections.singleton(entry.getValue());
			setMultiMap.put(entry.getKey(), Collections.unmodifiableSet(set));
		}
		return Collections.unmodifiableMap(setMultiMap);
	}
}
