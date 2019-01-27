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

package org.apache.flink.table.sources.parquet;

import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.dataformat.vector.VectorizedColumnBatch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetInputSplit;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.parquet.filter2.compat.RowGroupFilter.filterRowGroups;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.range;
import static org.apache.parquet.hadoop.ParquetFileReader.readFooter;
import static org.apache.parquet.hadoop.ParquetInputFormat.getFilter;

/**
 * This reader is used to read a {@link VectorizedColumnBatch} from input split, part of the code is referred
 * from Apache Spark and Hive.
 */
public class ParquetVectorizedReader extends RecordReader<Void, Object> {

	private Path file;
	private MessageType fileSchema;
	private MessageType requestedSchema;

	/**
	 * For each request column, the reader to read this column. This is NULL if this column
	 * is missing from the file, in which case we populate the attribute with NULL.
	 */
	protected VectorizedColumnReader[] columnReaders;
	/**
	 * The total number of rows this RecordReader will eventually read. The sum of the
	 * rows of all the row groups.
	 */
	protected long totalRowCount;
	/**
	 * The number of rows that have been returned.
	 */
	protected long rowsReturned;
	/**
	 * The number of rows that have been reading, including the current in flight row group.
	 */
	protected long totalCountLoadedSoFar;
	protected ParquetFileReader reader;
	protected VectorizedColumnBatch columnarBatch;
	protected InternalType[] fieldTypes;
	protected String[] fieldNames;

	public ParquetVectorizedReader(InternalType[] fieldTypes, String[] fieldNames) {
		super();
		this.fieldTypes = fieldTypes;
		this.fieldNames = fieldNames;
	}

	@Override
	public void initialize(
		InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
		// the inputSplit may be null during the split phase
		Configuration configuration = taskAttemptContext.getConfiguration();
		ParquetMetadata footer;
		List<BlockMetaData> blocks;
		ParquetInputSplit split = (ParquetInputSplit) inputSplit;
		this.file = split.getPath();
		long[] rowGroupOffsets = split.getRowGroupOffsets();

		// if task.side.metadata is set, rowGroupOffsets is null
		if (rowGroupOffsets == null) {
			// then we need to apply the predicate push down filter
			footer = readFooter(configuration, file, range(split.getStart(), split.getEnd()));
			MessageType fileSchema = footer.getFileMetaData().getSchema();
			FilterCompat.Filter filter = getFilter(configuration);
			blocks = filterRowGroups(filter, footer.getBlocks(), fileSchema);
		} else {
			// otherwise we find the row groups that were selected on the client
			footer = readFooter(configuration, file, NO_FILTER);
			Set<Long> offsets = new HashSet<>();
			for (long offset : rowGroupOffsets) {
				offsets.add(offset);
			}
			blocks = new ArrayList<>();
			for (BlockMetaData block : footer.getBlocks()) {
				if (offsets.contains(block.getStartingPos())) {
					blocks.add(block);
				}
			}
			// verify we found them all
			if (blocks.size() != rowGroupOffsets.length) {
				long[] foundRowGroupOffsets = new long[footer.getBlocks().size()];
				for (int i = 0; i < foundRowGroupOffsets.length; i++) {
					foundRowGroupOffsets[i] = footer.getBlocks().get(i).getStartingPos();
				}
				// this should never happen.
				// provide a good error message in case there's a bug
				throw new IllegalStateException(
					"All the offsets listed in the split should be found in the file."
					+ " expected: " + Arrays.toString(rowGroupOffsets)
					+ " found: " + blocks
					+ " out of: " + Arrays.toString(foundRowGroupOffsets)
					+ " in range " + split.getStart() + ", " + split.getEnd());
			}
		}

		this.fileSchema = footer.getFileMetaData().getSchema();

		this.requestedSchema = clipParquetSchema(fileSchema, fieldNames);

		this.reader = new ParquetFileReader(
			configuration, footer.getFileMetaData(), file, blocks, requestedSchema.getColumns());

		for (BlockMetaData block : blocks) {
			this.totalRowCount += block.getRowCount();
		}

		checkColumn();
		resultBatch();
	}

	private void checkColumn() throws IOException, UnsupportedOperationException {

		if (fieldTypes.length != requestedSchema.getFieldCount()) {
			throw new RuntimeException("The quality of field type is incompatible with the request schema!");
		}
		/**
		 * Check that the requested schema is supported.
		 */
		for (int i = 0; i < requestedSchema.getFieldCount(); ++i) {
			Type t = requestedSchema.getFields().get(i);
			if (!t.isPrimitive() || t.isRepetition(Type.Repetition.REPEATED)) {
				throw new UnsupportedOperationException("Complex types not supported.");
			}

			String[] colPath = requestedSchema.getPaths().get(i);
			if (fileSchema.containsPath(colPath)) {
				ColumnDescriptor fd = fileSchema.getColumnDescription(colPath);
				if (!fd.equals(requestedSchema.getColumns().get(i))) {
					throw new UnsupportedOperationException("Schema evolution not supported.");
				}
			} else {
				if (requestedSchema.getColumns().get(i).getMaxDefinitionLevel() == 0) {
					// Column is missing in data but the required data is non-nullable. This file is invalid.
					throw new IOException("Required column is missing in data file. Col: " + Arrays.toString(colPath));
				}
			}
		}
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		return nextBatch();
	}

	void initBatch() {
		columnarBatch = VectorizedColumnBatch.allocate(fieldTypes);
	}

	public VectorizedColumnBatch resultBatch() throws IOException {
		if (columnarBatch == null) {
			initBatch();
		}
		return columnarBatch;
	}

	@Override
	public Void getCurrentKey() throws IOException, InterruptedException {
		return null;
	}

	@Override
	public Object getCurrentValue() throws IOException, InterruptedException {
		return columnarBatch;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return (float) rowsReturned / totalRowCount;
	}

	@Override
	public void close() throws IOException {
		if (reader != null) {
			reader.close();
			reader = null;
		}
	}

	/**
	 * Advances to the next batch of rows. Returns false if there are no more.
	 */
	protected boolean nextBatch() throws IOException {
		columnarBatch.reset();
		if (rowsReturned >= totalRowCount) {
			return false;
		}
		checkEndOfRowGroup();

		int num = (int) Math.min(VectorizedColumnBatch.MAX_SIZE, totalCountLoadedSoFar - rowsReturned);
		for (int i = 0; i < columnReaders.length; ++i) {
			if (columnReaders[i] == null) {
				continue;
			}
			columnReaders[i].readColumnBatch(num, columnarBatch.columns[i], fieldTypes[i]);
		}
		rowsReturned += num;
		columnarBatch.setNumRows(num);
		return true;
	}

	protected void checkEndOfRowGroup() throws IOException {
		if (rowsReturned != totalCountLoadedSoFar) {
			return;
		}
		PageReadStore pages = reader.readNextRowGroup();
		if (pages == null) {
			throw new IOException("expecting more rows but reached last block. Read "
					+ rowsReturned + " out of " + totalRowCount);
		}
		List<ColumnDescriptor> columns = requestedSchema.getColumns();
		columnReaders = new VectorizedColumnReader[columns.size()];
		for (int i = 0; i < columns.size(); ++i) {
			columnReaders[i] = new VectorizedColumnReader(columns.get(i),
					pages.getPageReader(columns.get(i)));
		}
		totalCountLoadedSoFar += pages.getRowCount();
	}

	/**
	 * Clips `parquetSchema` according to `fieldNames`.
	 */
	private MessageType clipParquetSchema(GroupType parquetSchema, String[] fieldNames) {
		Type[] types = new Type[fieldNames.length];
		for (int i = 0; i < fieldNames.length; ++i) {
			String fieldName = fieldNames[i];
			if (parquetSchema.getFieldIndex(fieldName) < 0) {
				throw new IllegalArgumentException(fieldName + " does not exist");
			}
			types[i] = parquetSchema.getType(fieldName);
		}
		return Types.buildMessage().addFields(types).named("flink-parquet");
	}
}
