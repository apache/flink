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

package org.apache.flink.orc;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static org.apache.flink.orc.OrcUtils.fillRows;

/**
 * InputFormat to read ORC data.
 * For Optimization, reading is done in batch instead of a single row.
 */
public class RowOrcInputFormat
	extends FileInputFormat<Row>
	implements ResultTypeQueryable<Row> {

	private static final Logger LOG = LoggerFactory.getLogger(RowOrcInputFormat.class);
	private static final int BATCH_SIZE = 1024;

	private org.apache.hadoop.conf.Configuration config;
	private TypeDescription schema;
	private int[] fieldMapping;

	private transient RowTypeInfo rowType;
	private transient RecordReader orcRowsReader;
	private transient VectorizedRowBatch rowBatch;
	private transient Row[] rows;

	private transient int rowInBatch;

	public RowOrcInputFormat(String path, String schemaString, Configuration orcConfig) {
		this(path, TypeDescription.fromString(schemaString), orcConfig);
	}

	public RowOrcInputFormat(String path, TypeDescription orcSchema, Configuration orcConfig) {
		super(new Path(path));
		this.unsplittable = false;
		this.schema = orcSchema;
		this.rowType = (RowTypeInfo) OrcUtils.schemaToTypeInfo(schema);
		this.config = orcConfig;

		this.fieldMapping = new int[this.schema.getChildren().size()];
		for (int i = 0; i < fieldMapping.length; i++) {
			this.fieldMapping[i] = i;
		}

	}

	public void setFieldMapping(int[] fieldMapping) {
		this.fieldMapping = fieldMapping;
		// adapt result type

		TypeInformation[] fieldTypes = new TypeInformation[fieldMapping.length];
		String[] fieldNames = new String[fieldMapping.length];
		for (int i = 0; i < fieldMapping.length; i++) {
			fieldTypes[i] = this.rowType.getTypeAt(fieldMapping[i]);
			fieldNames[i] = this.rowType.getFieldNames()[fieldMapping[i]];
		}
		this.rowType = new RowTypeInfo(fieldTypes, fieldNames);
	}

	private boolean[] computeProjectionMask() {
		boolean[] projectionMask = new boolean[schema.getMaximumId() + 1];
		for (int inIdx : fieldMapping) {
			TypeDescription fieldSchema = schema.getChildren().get(inIdx);
			for (int i = fieldSchema.getId(); i <= fieldSchema.getMaximumId(); i++) {
				projectionMask[i] = true;
			}
		}
		return projectionMask;
	}

	@Override
	public void openInputFormat() throws IOException {
		super.openInputFormat();
		this.rows = new Row[BATCH_SIZE];
		for (int i = 0; i < BATCH_SIZE; i++) {
			rows[i] = new Row(fieldMapping.length);
		}
	}

	@Override
	public void open(FileInputSplit fileSplit) throws IOException {

		this.currentSplit = fileSplit;
		Preconditions.checkArgument(this.splitStart == 0, "ORC files must be read from the start.");

		if (LOG.isDebugEnabled()) {
			LOG.debug("Opening ORC file " + fileSplit.getPath());
		}

		org.apache.hadoop.fs.Path hPath = new org.apache.hadoop.fs.Path(fileSplit.getPath().getPath());

		Reader orcReader = OrcFile.createReader(hPath, OrcFile.readerOptions(config));

		Reader.Options options = orcReader.options()
			.range(fileSplit.getStart(), fileSplit.getLength())
			.useZeroCopy(OrcConf.USE_ZEROCOPY.getBoolean(config))
			.skipCorruptRecords(OrcConf.SKIP_CORRUPT_DATA.getBoolean(config))
			.tolerateMissingSchema(OrcConf.TOLERATE_MISSING_SCHEMA.getBoolean(config));

		options.include(computeProjectionMask());

		// check that schema of file is as expected
		if (!this.schema.equals(orcReader.getSchema())) {

			throw new RuntimeException("Invalid schema for file at " + this.filePath +
				" Expected:" + this.schema + " Actual: " + orcReader.getSchema());
		}

		this.orcRowsReader = orcReader.rows(options);

		// assign ids
		this.schema.getId();

		this.rowBatch = schema.createRowBatch(BATCH_SIZE);
		rowInBatch = 0;
	}

	@Override
	public void close() throws IOException {

		if (orcRowsReader != null) {
			this.orcRowsReader.close();
		}
		this.orcRowsReader = null;

	}

	@Override
	public void closeInputFormat() throws IOException {
		this.rows = null;
		this.rows = null;
		this.schema = null;
		this.rowBatch = null;
	}

	@Override
	public boolean reachedEnd() throws IOException {
		return !ensureBatch();
	}

	private boolean ensureBatch() throws IOException {

		if (rowInBatch >= rowBatch.size) {
			rowInBatch = 0;
			boolean moreRows = orcRowsReader.nextBatch(rowBatch);

			if (moreRows) {
				// read rows
				fillRows(rows, schema, rowBatch, fieldMapping);
			}
			return moreRows;
		}

		return true;
	}

	@Override
	public Row nextRecord(Row reuse) throws IOException {
		return rows[this.rowInBatch++];
	}

	@Override
	public TypeInformation<Row> getProducedType() {
		return rowType;
	}

	// --------------------------------------------------------------------------------------------
	//  Custom serialization methods
	// --------------------------------------------------------------------------------------------

	private void writeObject(ObjectOutputStream out) throws IOException {
		this.config.write(out);
		out.writeUTF(schema.toString());

		out.writeInt(fieldMapping.length);
		for (int f : fieldMapping) {
			out.writeInt(f);
		}

	}

	@SuppressWarnings("unchecked")
	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {

		org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
		configuration.readFields(in);

		if (this.config == null) {
			this.config = configuration;
		}
		this.schema = TypeDescription.fromString(in.readUTF());

		this.fieldMapping = new int[in.readInt()];
		for (int i = 0; i < fieldMapping.length; i++) {
			this.fieldMapping[i] = in.readInt();
		}

	}

}
