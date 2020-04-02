/*
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
 */

package org.apache.flink.formats.parquet.vector.reader;

import org.apache.flink.formats.parquet.vector.ParquetDictionary;
import org.apache.flink.table.dataformat.vector.writable.WritableColumnVector;
import org.apache.flink.table.dataformat.vector.writable.WritableIntVector;

import org.apache.parquet.Preconditions;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.impl.ColumnReaderImpl;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.schema.PrimitiveType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.apache.parquet.column.ValuesType.REPETITION_LEVEL;

/**
 * Abstract {@link ColumnReader}.
 * See {@link ColumnReaderImpl}, part of the code is referred from Apache Spark and Apache Parquet.
 */
public abstract class AbstractColumnReader<VECTOR extends WritableColumnVector>
		implements ColumnReader<VECTOR> {

	private static final Logger LOG = LoggerFactory.getLogger(AbstractColumnReader.class);

	private final PageReader pageReader;

	/**
	 * The dictionary, if this column has dictionary encoding.
	 */
	protected final Dictionary dictionary;

	/**
	 * Maximum definition level for this column.
	 */
	protected final int maxDefLevel;

	protected final ColumnDescriptor descriptor;

	/**
	 * Total number of values read.
	 */
	private long valuesRead;

	/**
	 * value that indicates the end of the current page. That is, if valuesRead ==
	 * endOfPageValueCount, we are at the end of the page.
	 */
	private long endOfPageValueCount;

	/**
	 * If true, the current page is dictionary encoded.
	 */
	private boolean isCurrentPageDictionaryEncoded;

	/**
	 * Total values in the current page.
	 */
	private int pageValueCount;

	/*
	 * Input streams:
	 * 1.Run length encoder to encode every data, so we have run length stream to get
	 *  run length information.
	 * 2.Data maybe is real data, maybe is dictionary ids which need be decode to real
	 *  data from Dictionary.
	 *
	 * Run length stream ------> Data stream
	 *                  |
	 *                   ------> Dictionary ids stream
	 */

	/**
	 * Run length decoder for data and dictionary.
	 */
	protected RunLengthDecoder runLenDecoder;

	/**
	 * Data input stream.
	 */
	ByteBufferInputStream dataInputStream;

	/**
	 * Dictionary decoder to wrap dictionary ids input stream.
	 */
	private RunLengthDecoder dictionaryIdsDecoder;

	public AbstractColumnReader(
			ColumnDescriptor descriptor,
			PageReader pageReader) throws IOException {
		this.descriptor = descriptor;
		this.pageReader = pageReader;
		this.maxDefLevel = descriptor.getMaxDefinitionLevel();

		DictionaryPage dictionaryPage = pageReader.readDictionaryPage();
		if (dictionaryPage != null) {
			try {
				this.dictionary = dictionaryPage.getEncoding().initDictionary(descriptor, dictionaryPage);
				this.isCurrentPageDictionaryEncoded = true;
			} catch (IOException e) {
				throw new IOException("could not decode the dictionary for " + descriptor, e);
			}
		} else {
			this.dictionary = null;
			this.isCurrentPageDictionaryEncoded = false;
		}
		/*
		 * Total number of values in this column (in this row group).
		 */
		long totalValueCount = pageReader.getTotalValueCount();
		if (totalValueCount == 0) {
			throw new IOException("totalValueCount == 0");
		}
	}

	protected void checkTypeName(PrimitiveType.PrimitiveTypeName expectedName) {
		PrimitiveType.PrimitiveTypeName actualName = descriptor.getPrimitiveType().getPrimitiveTypeName();
		Preconditions.checkArgument(
				actualName == expectedName,
				"Expected type name: %s, actual type name: %s",
				expectedName,
				actualName);
	}

	/**
	 * Reads `total` values from this columnReader into column.
	 */
	@Override
	public final void readToVector(int readNumber, VECTOR vector) throws IOException {
		int rowId = 0;
		WritableIntVector dictionaryIds = null;
		if (dictionary != null) {
			dictionaryIds = vector.reserveDictionaryIds(readNumber);
		}
		while (readNumber > 0) {
			// Compute the number of values we want to read in this page.
			int leftInPage = (int) (endOfPageValueCount - valuesRead);
			if (leftInPage == 0) {
				DataPage page = pageReader.readPage();
				if (page instanceof DataPageV1) {
					readPageV1((DataPageV1) page);
				} else if (page instanceof DataPageV2) {
					readPageV2((DataPageV2) page);
				} else {
					throw new RuntimeException("Unsupported page type: " + page.getClass());
				}
				leftInPage = (int) (endOfPageValueCount - valuesRead);
			}
			int num = Math.min(readNumber, leftInPage);
			if (isCurrentPageDictionaryEncoded) {
				// Read and decode dictionary ids.
				runLenDecoder.readDictionaryIds(
						num, dictionaryIds, vector, rowId, maxDefLevel, this.dictionaryIdsDecoder);

				if (vector.hasDictionary() || (rowId == 0 && supportLazyDecode())) {
					// Column vector supports lazy decoding of dictionary values so just set the dictionary.
					// We can't do this if rowId != 0 AND the column doesn't have a dictionary (i.e. some
					// non-dictionary encoded values have already been added).
					vector.setDictionary(new ParquetDictionary(dictionary));
				} else {
					readBatchFromDictionaryIds(rowId, num, vector, dictionaryIds);
				}
			} else {
				if (vector.hasDictionary() && rowId != 0) {
					// This batch already has dictionary encoded values but this new page is not. The batch
					// does not support a mix of dictionary and not so we will decode the dictionary.
					readBatchFromDictionaryIds(0, rowId, vector, vector.getDictionaryIds());
				}
				vector.setDictionary(null);
				readBatch(rowId, num, vector);
			}

			valuesRead += num;
			rowId += num;
			readNumber -= num;
		}
	}

	private void readPageV1(DataPageV1 page) throws IOException {
		this.pageValueCount = page.getValueCount();
		ValuesReader rlReader = page.getRlEncoding().getValuesReader(descriptor, REPETITION_LEVEL);

		// Initialize the decoders.
		if (page.getDlEncoding() != Encoding.RLE && descriptor.getMaxDefinitionLevel() != 0) {
			throw new UnsupportedOperationException("Unsupported encoding: " + page.getDlEncoding());
		}
		int bitWidth = BytesUtils.getWidthFromMaxInt(descriptor.getMaxDefinitionLevel());
		this.runLenDecoder = new RunLengthDecoder(bitWidth);
		try {
			BytesInput bytes = page.getBytes();
			ByteBufferInputStream in = bytes.toInputStream();
			rlReader.initFromPage(pageValueCount, in);
			this.runLenDecoder.initFromStream(pageValueCount, in);
			prepareNewPage(page.getValueEncoding(), in);
		} catch (IOException e) {
			throw new IOException("could not read page " + page + " in col " + descriptor, e);
		}
	}

	private void readPageV2(DataPageV2 page) throws IOException {
		this.pageValueCount = page.getValueCount();

		int bitWidth = BytesUtils.getWidthFromMaxInt(descriptor.getMaxDefinitionLevel());
		// do not read the length from the stream. v2 pages handle dividing the page bytes.
		this.runLenDecoder = new RunLengthDecoder(bitWidth, false);
		this.runLenDecoder.initFromStream(
				this.pageValueCount, page.getDefinitionLevels().toInputStream());
		try {
			prepareNewPage(page.getDataEncoding(), page.getData().toInputStream());
		} catch (IOException e) {
			throw new IOException("could not read page " + page + " in col " + descriptor, e);
		}
	}

	private void prepareNewPage(
			Encoding dataEncoding,
			ByteBufferInputStream in) throws IOException {
		this.endOfPageValueCount = valuesRead + pageValueCount;
		if (dataEncoding.usesDictionary()) {
			if (dictionary == null) {
				throw new IOException(
						"could not read page in col " + descriptor +
								" as the dictionary was missing for encoding " + dataEncoding);
			}
			@SuppressWarnings("deprecation")
			Encoding plainDict = Encoding.PLAIN_DICTIONARY; // var to allow warning suppression
			if (dataEncoding != plainDict && dataEncoding != Encoding.RLE_DICTIONARY) {
				throw new UnsupportedOperationException("Unsupported encoding: " + dataEncoding);
			}
			this.dataInputStream = null;
			this.dictionaryIdsDecoder = new RunLengthDecoder();
			try {
				this.dictionaryIdsDecoder.initFromStream(pageValueCount, in);
			} catch (IOException e) {
				throw new IOException("could not read dictionary in col " + descriptor, e);
			}
			this.isCurrentPageDictionaryEncoded = true;
		} else {
			if (dataEncoding != Encoding.PLAIN) {
				throw new UnsupportedOperationException("Unsupported encoding: " + dataEncoding);
			}
			this.dictionaryIdsDecoder = null;
			LOG.debug("init from page at offset {} for length {}", in.position(), in.available());
			this.dataInputStream = in.remainingStream();
			this.isCurrentPageDictionaryEncoded = false;
		}

		afterReadPage();
	}

	final ByteBuffer readDataBuffer(int length) {
		try {
			return dataInputStream.slice(length).order(ByteOrder.LITTLE_ENDIAN);
		} catch (IOException e) {
			throw new ParquetDecodingException("Failed to read " + length + " bytes", e);
		}
	}

	/**
	 * After read a page, we may need some initialization.
	 */
	protected void afterReadPage() {}

	/**
	 * Support lazy dictionary ids decode. See more in {@link ParquetDictionary}.
	 * If return false, we will decode all the data first.
	 */
	protected boolean supportLazyDecode() {
		return true;
	}

	/**
	 * Read batch from {@link #runLenDecoder} and {@link #dataInputStream}.
	 */
	protected abstract void readBatch(int rowId, int num, VECTOR column);

	/**
	 * Decode dictionary ids to data.
	 * From {@link #runLenDecoder} and {@link #dictionaryIdsDecoder}.
	 */
	protected abstract void readBatchFromDictionaryIds(
			int rowId,
			int num,
			VECTOR column,
			WritableIntVector dictionaryIds);
}
