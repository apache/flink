/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.flink.languagebinding.api.java.common;

import java.util.Arrays;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * Container for all generic information related to operations. This class contains the absolute minimum fields that are
 * required for all operations. This class should be extended to contain any additional fields required on a
 * per-language basis.
 */
public abstract class OperationInfo {
	public int parentID; //DataSet that an operation is applied on
	public int otherID; //secondary DataSet
	public int setID; //ID for new DataSet
	public int[] keys1; //join/cogroup keys
	public int[] keys2; //join/cogroup keys
	public TypeInformation<?> types; //typeinformation about output type
	public ProjectionEntry[] projections; //projectFirst/projectSecond

	public class ProjectionEntry {
		public ProjectionSide side;
		public int[] keys;

		public ProjectionEntry(ProjectionSide side, int[] keys) {
			this.side = side;
			this.keys = keys;
		}

		@Override
		public String toString() {
			return side + " - " + Arrays.toString(keys);
		}
	}

	public enum ProjectionSide {
		FIRST,
		SECOND
	}

	public enum DatasizeHint {
		NONE,
		TINY,
		HUGE
	}
}
