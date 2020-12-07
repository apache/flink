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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.orc.OrcFilters.ColumnPredicate;
import org.apache.flink.orc.OrcFilters.Not;
import org.apache.flink.orc.OrcFilters.Or;
import org.apache.flink.orc.OrcFilters.Predicate;

import org.apache.hadoop.conf.Configuration;
import org.apache.orc.TypeDescription;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;

/**
 * InputFormat to read ORC files.
 */
public abstract class OrcInputFormat<T> extends FileInputFormat<T> {

	// the number of fields rows to read in a batch
	protected int batchSize;
	// the configuration to read with
	protected Configuration conf;
	// the schema of the ORC files to read
	protected TypeDescription schema;

	// the fields of the ORC schema that the returned Rows are composed of.
	protected int[] selectedFields;

	protected ArrayList<Predicate> conjunctPredicates = new ArrayList<>();

	protected transient OrcSplitReader<T, ?> reader;

	/**
	 * Creates an OrcInputFormat.
	 *
	 * @param path The path to read ORC files from.
	 * @param orcSchema The schema of the ORC files as ORC TypeDescription.
	 * @param orcConfig The configuration to read the ORC files with.
	 * @param batchSize The number of Row objects to read in a batch.
	 */
	public OrcInputFormat(Path path, TypeDescription orcSchema, Configuration orcConfig, int batchSize) {
		super(path);

		// configure OrcInputFormat
		this.schema = orcSchema;
		this.conf = orcConfig;
		this.batchSize = batchSize;

		// set default selection mask, i.e., all fields.
		this.selectedFields = new int[this.schema.getChildren().size()];
		for (int i = 0; i < selectedFields.length; i++) {
			this.selectedFields[i] = i;
		}
	}

	/**
	 * Selects the fields from the ORC schema that are returned by InputFormat.
	 *
	 * @param selectedFields The indices of the fields of the ORC schema that are returned by the InputFormat.
	 */
	public void selectFields(int... selectedFields) {
		// set field mapping
		this.selectedFields = selectedFields;
	}

	/**
	 * Adds a filter predicate to reduce the number of rows to be returned by the input format.
	 * Multiple conjunctive predicates can be added by calling this method multiple times.
	 *
	 * <p>Note: Predicates can significantly reduce the amount of data that is read.
	 * However, the OrcInputFormat does not guarantee that all returned rows qualify the
	 * predicates. Moreover, predicates are only applied if the referenced field is among the
	 * selected fields.
	 *
	 * @param predicate The filter predicate.
	 */
	public void addPredicate(Predicate predicate) {
		// validate
		validatePredicate(predicate);
		// add predicate
		this.conjunctPredicates.add(predicate);
	}

	private void validatePredicate(Predicate pred) {
		if (pred instanceof ColumnPredicate) {
			// check column name
			String colName = ((ColumnPredicate) pred).columnName;
			if (!this.schema.getFieldNames().contains(colName)) {
				throw new IllegalArgumentException("Predicate cannot be applied. " +
					"Column '" + colName + "' does not exist in ORC schema.");
			}
		} else if (pred instanceof Not) {
			validatePredicate(((Not) pred).child());
		} else if (pred instanceof Or) {
			for (Predicate p : ((Or) pred).children()) {
				validatePredicate(p);
			}
		}
	}

	@Override
	public void close() throws IOException {
		if (reader != null) {
			this.reader.close();
		}
		this.reader = null;
	}

	@Override
	public void closeInputFormat() throws IOException{
		this.schema = null;
	}

	@Override
	public boolean reachedEnd() throws IOException {
		return reader.reachedEnd();
	}

	@Override
	public T nextRecord(T reuse) throws IOException {
		return reader.nextRecord(reuse);
	}

	@VisibleForTesting
	OrcSplitReader<T, ?> getReader() {
		return reader;
	}

	// --------------------------------------------------------------------------------------------
	//  Custom serialization methods
	// --------------------------------------------------------------------------------------------

	private void writeObject(ObjectOutputStream out) throws IOException {
		out.writeInt(batchSize);
		this.conf.write(out);
		out.writeUTF(schema.toString());

		out.writeInt(selectedFields.length);
		for (int f : selectedFields) {
			out.writeInt(f);
		}

		out.writeInt(conjunctPredicates.size());
		for (Predicate p : conjunctPredicates) {
			out.writeObject(p);
		}
	}

	@SuppressWarnings("unchecked")
	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		batchSize = in.readInt();
		Configuration configuration = new Configuration();
		configuration.readFields(in);

		if (this.conf == null) {
			this.conf = configuration;
		}
		this.schema = TypeDescription.fromString(in.readUTF());

		this.selectedFields = new int[in.readInt()];
		for (int i = 0; i < selectedFields.length; i++) {
			this.selectedFields[i] = in.readInt();
		}

		this.conjunctPredicates = new ArrayList<>();
		int numPreds = in.readInt();
		for (int i = 0; i < numPreds; i++) {
			conjunctPredicates.add((Predicate) in.readObject());
		}
	}

	@Override
	public boolean supportsMultiPaths() {
		return true;
	}

	// --------------------------------------------------------------------------------------------
	//  Getter methods for tests
	// --------------------------------------------------------------------------------------------

	@VisibleForTesting
	Configuration getConfiguration() {
		return conf;
	}

	@VisibleForTesting
	int getBatchSize() {
		return batchSize;
	}

	@VisibleForTesting
	String getSchema() {
		return schema.toString();
	}
}
