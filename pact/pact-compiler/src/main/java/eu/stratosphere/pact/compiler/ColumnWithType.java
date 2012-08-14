/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.compiler;

import eu.stratosphere.pact.common.type.Key;


/**
 * 
 */
public class ColumnWithType
{
	private final Class<? extends Key> columnType;
	
	private final int columnIndex;

	public ColumnWithType(int columnIndex, Class<? extends Key> columnType) {
		this.columnType = columnType;
		this.columnIndex = columnIndex;
	}
	
	/**
	 * Gets the columnType from this column.
	 *
	 * @return The column type.
	 */
	public Class<? extends Key> getColumnType() {
		return columnType;
	}
	
	/**
	 * Gets the column index from this column.
	 *
	 * @return The column index.
	 */
	public int getColumnIndex() {
		return columnIndex;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return this.columnIndex;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj)
	{
		if (obj != null && obj instanceof ColumnWithType) {
			final ColumnWithType cwt = (ColumnWithType) obj;
			if (this.columnIndex == cwt.columnIndex) {
				if (this.columnType != cwt.columnType) {
					// DEBUG ONLY!!! Should never happen, if used properly.
					throw new RuntimeException("Comparing columns with equal position but incompatible type.");
				}
				return true;
			}
		}
		
		return false;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "Column " + this.columnIndex + " (" + this.columnType.getName() + ')';
	}
}