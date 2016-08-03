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

package org.apache.flink.streaming.connectors.hbase;

import org.apache.flink.util.Preconditions;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;

import java.util.ArrayList;
import java.util.List;

/**
 *  This class represents a list of {@link MutationAction}s you will take when writing
 *  an input value of {@link HBaseSink} to a row in a HBase table.
 *  Each {@link MutationAction} can create an HBase {@link Mutation} operation type
 *  including {@link Put}, {@link Increment}, {@link Append} and {@link Delete}.
 */
public class MutationActions {
	private final List<MutationAction> actions;

	public MutationActions() {
		this.actions = new ArrayList<>();
	}

	/**
	 * Create a new list of HBase {@link Mutation}s.
	 *
	 * @param rowKey row that the created {@link Mutation} list is applied to
	 * @param writeToWAL enable WAL
	 * @return a list of HBase {@link Mutation}s
	 */
	public List<Mutation> createMutations(byte[] rowKey, boolean writeToWAL) {
		Preconditions.checkArgument(rowKey != null, "Row key cannot be null.");
		List<Mutation> mutations = new ArrayList<>();
		Put put = null;
		Increment increment = null;
		Append append = null;
		Delete deleteRow = null;
		Delete delete = null;
		for (MutationAction action : actions) {
			switch (action.getType()) {
				case PUT:
					if (put == null) {
						put = new Put(rowKey);
						mutations.add(put);
					}
					if (action.getTs() == -1) {
						put.addColumn(action.getFamily(), action.getQualifier(), action.getValue());
					} else {
						put.addColumn(action.getFamily(), action.getQualifier(), action.getTs(), action.getValue());
					}
					break;

				case INCREMENT:
					if (increment == null) {
						increment = new Increment(rowKey);
						mutations.add(increment);
					}
					increment.addColumn(action.getFamily(), action.getQualifier(), action.getIncrement());
					break;

				case APPEND:
					if (append == null) {
						append = new Append(rowKey);
						mutations.add(append);
					}
					append.add(action.getFamily(), action.getQualifier(), action.getValue());
					break;

				case DELETE_ROW:
					if (deleteRow == null) {
						deleteRow = new Delete(rowKey, action.getTs());
						mutations.add(deleteRow);
					} else {
						deleteRow.setTimestamp(Math.max(deleteRow.getTimeStamp(), action.getTs()));
					}
					break;

				case DELETE_FAMILY:
					if (delete == null) {
						delete = new Delete(rowKey);
						mutations.add(delete);
					}
					delete.addFamily(action.getFamily(), action.getTs());
					break;

				case DELETE_COLUMNS:
					if (delete == null) {
						delete = new Delete(rowKey);
						mutations.add(delete);
					}
					delete.addColumns(action.getFamily(), action.getQualifier(), action.getTs());
					break;

				case DELETE_COLUMN:
					if (delete == null) {
						delete = new Delete(rowKey);
						mutations.add(delete);
					}
					if (action.getTs() == -1) {
						delete.addColumn(action.getFamily(), action.getQualifier());
					} else {
						delete.addColumn(action.getFamily(), action.getQualifier(), action.getTs());
					}
					break;

				default:
					throw new IllegalArgumentException("Cannot process such action type: " + action.getType());
			}
		}
		Durability durability = writeToWAL ? Durability.SYNC_WAL : Durability.SKIP_WAL;
		for (Mutation mutation : mutations) {
			mutation.setDurability(durability);
		}
		return mutations;
	}

	/**
	 * Add to {@link MutationActions} a {@link MutationAction} of type {@link MutationAction.Type#PUT}. which will
	 * create an HBase {@link Put} operation for a specified row with specified timestamp.
	 *
     * @param family family name
	 * @param qualifier column qualifier
	 * @param value column value
	 * @param timestamp version timestamp
	 * @return this
	 */
	public MutationActions addPut(byte[] family, byte[] qualifier, byte[] value, long timestamp) {
		actions.add(new MutationAction(family, qualifier, value, timestamp, 0, MutationAction.Type.PUT));
		return this;
	}

	/**
	 * Add to {@link MutationActions} a {@link MutationAction} of type {@link MutationAction.Type#PUT}, which will
	 * create an HBase {@link Put} operation for a specified row with no timestamp specified at client side.
	 *
	 * @param family family name
	 * @param qualifier column qualifier
	 * @param value column value
	 * @return this
	 */
	public MutationActions addPut(byte[] family, byte[] qualifier, byte[] value) {
		actions.add(new MutationAction(family, qualifier, value, 0, MutationAction.Type.PUT));
		return this;
	}

	/**
 	 * Add to {@link MutationActions} a {@link MutationAction} of type {@link MutationAction.Type#APPEND}, which will
	 * create an HBase {@link Append} operation for a specified row.
	 *
	 * @param family family name
	 * @param qualifier column qualifier
	 * @param value column value
	 * @return this
	 */
	public MutationActions addAppend(byte[] family, byte[] qualifier, byte[] value) {
		actions.add(new MutationAction(family, qualifier, value, 0, MutationAction.Type.APPEND));
		return this;
	}

	/**
	 * Add to {@link MutationActions} a {@link MutationAction} of type {@link MutationAction.Type#INCREMENT}, which will
	 * create an HBase {@link Increment} operation for a specified row with the specified amount.
	 *
	 * @param family family name
	 * @param qualifier column qualifier
	 * @param amount amount to increment by
	 * @return this
	 */
	public MutationActions addIncrement(byte[] family, byte[] qualifier, long amount) {
		actions.add(new MutationAction(family, qualifier, null, amount, MutationAction.Type.INCREMENT));
		return this;
	}

	/**
	 * Add to {@link MutationActions} a {@link MutationAction} of type {@link MutationAction.Type#DELETE_ROW}, which will
	 * create an HBase {@link Delete} operation that deletes all columns in all families of the specified row with
	 * a timestamp less than or equal to the specified timestamp.
	 *
	 * @param timestamp version timestamp
	 * @return this
	 */
	public MutationActions addDeleteRow(long timestamp) {
		actions.add(new MutationAction(null, null, null, timestamp, 0, MutationAction.Type.DELETE_ROW));
		return this;
	}

	/**
	 * Add to {@link MutationActions} a {@link MutationAction} of type {@link MutationAction.Type#DELETE_ROW}, which will
	 * create an HBase {@link Delete} operation that deletes everything associated with the specified row (all versions
	 * of all columns in all families).
	 *
	 * @return this
	 */
	public MutationActions addDeleteRow() {
		actions.add(new MutationAction(null, null, null, Long.MAX_VALUE, 0, MutationAction.Type.DELETE_ROW));
		return this;
	}

	/**
 	 * Add to {@link MutationActions} a {@link MutationAction} of type {@link MutationAction.Type#DELETE_FAMILY},
	 * which will create an HBase {@link Delete} operation that deletes versions of the specified family with
	 * timestamps less than or equal to the specified timestamp.
	 *
	 * @param family family name
	 * @param timestamp version timestamp
	 * @return this
	 */
	public MutationActions addDeleteFamily(byte[] family, long timestamp) {
		actions.add(new MutationAction(family, null, null, timestamp, 0, MutationAction.Type.DELETE_FAMILY));
		return this;
	}

	/**
	 * Add to {@link MutationActions} a {@link MutationAction} of type {@link MutationAction.Type#DELETE_FAMILY},
	 * which will create an HBase {@link Delete} operation that deletes all versions of the specified family.
	 *
	 * @param family family name
	 * @return this
	 */
	public MutationActions addDeleteFamily(byte[] family) {
		actions.add(new MutationAction(family, null, null, Long.MAX_VALUE, 0, MutationAction.Type.DELETE_FAMILY));
		return this;
	}

	/**
	 * Add to {@link MutationActions} a {@link MutationAction} of type {@link MutationAction.Type#DELETE_COLUMNS},
	 * which will create an HBase {@link Delete} operation that deletes versions of the specified column with
	 * timestamps less than or equal to the specified timestamp.
	 *
	 * @param family family name
	 * @param qualifier column qualifier
	 * @param timestamp version timestamp
	 * @return this
	 */
	public MutationActions addDeleteColumns(byte[] family, byte[] qualifier, long timestamp) {
		actions.add(new MutationAction(family, qualifier, null, timestamp, 0, MutationAction.Type.DELETE_COLUMNS));
		return this;
	}

	/**
	 * Add to {@link MutationActions} a {@link MutationAction} of type {@link MutationAction.Type#DELETE_COLUMNS},
	 * which will create an HBase {@link Delete} operation that deletes all versions of the specified column.
	 *
	 * @param family family name
	 * @param qualifier column qualifier
	 * @return this
	 */
	public MutationActions addDeleteColumns(byte[] family, byte[] qualifier) {
		actions.add(new MutationAction(family, qualifier, null, Long.MAX_VALUE, 0, MutationAction.Type.DELETE_COLUMNS));
		return this;
	}

	/**
	 * Add to {@link MutationActions} a {@link MutationAction} of type {@link MutationAction.Type#DELETE_COLUMN},
	 * which will create an HBase {@link Delete} operation that deletes the specified version of the specified column.
	 *
	 * @param family family name
	 * @param qualifier column qualifier
	 * @param timestamp version timestamp
	 * @return this
	 */
	public MutationActions addDeleteColumn(byte[] family, byte[] qualifier, long timestamp) {
		actions.add(new MutationAction(family, qualifier, null, timestamp, 0, MutationAction.Type.DELETE_COLUMN));
		return this;
	}

	/**
	 * Add to {@link MutationActions} a {@link MutationAction} of type {@link MutationAction.Type#DELETE_COLUMN},
	 * which will create an HBase {@link Delete} operation that deletes the latest version of the specified column.
	 *
	 * @param family family name
	 * @param qualifier column qualifier
	 * @return this
	 */
	public MutationActions addDeleteColumn(byte[] family, byte[] qualifier) {
		actions.add(new MutationAction(family, qualifier, null, -1, 0, MutationAction.Type.DELETE_COLUMN));
		return this;
	}

	private static class MutationAction {

		enum Type {
			PUT,
			INCREMENT,
			APPEND,
			DELETE_ROW,
			DELETE_FAMILY,
			DELETE_COLUMNS,
			DELETE_COLUMN
		}

		private final byte[] family;
		private final byte[] qualifier;
		private final byte[] value;
		private final long ts;
		private final long increment;
		private final Type type;

		MutationAction(byte[] family, byte[] qualifier, byte[] value, long timestamp, long amount, Type type) {
			Preconditions.checkArgument(timestamp >= 0, "Timestamp cannot be negative.");
			this.family = family;
			this.qualifier = qualifier;
			this.value = value;
			this.ts = timestamp;
			this.increment = amount;
			this.type = type;
		}

		MutationAction(byte[] family, byte[] qualifier, byte[] value, long amount, Type type) {
			this.family = family;
			this.qualifier = qualifier;
			this.value = value;
			this.ts = -1;
			this.increment = amount;
			this.type = type;
		}

		byte[] getFamily() {
			return family;
		}

		byte[] getQualifier() {
			return qualifier;
		}

		byte[] getValue() {
			return value;
		}

		long getTs() {
			return ts;
		}

		long getIncrement() {
			return increment;
		}

		Type getType() {
			return type;
		}
	}
}
