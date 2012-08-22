/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.common.util;

import java.util.Collection;
import java.util.HashSet;

public class FieldSet  extends HashSet<Integer>{

	private static final long serialVersionUID = -6333143284628275141L;

	public FieldSet() {
	}
	
	public FieldSet(int columnIndex) {
		this.add(columnIndex);
	}
	
	public FieldSet(int[] columnIndexes) {
		for (int columnIndex : columnIndexes)
			this.add(columnIndex);
	}
	
	public FieldSet(Collection<Integer> o) {
		this.addAll(o);
	}
	
	public FieldSet(Collection<Integer> o1, Collection<Integer> o2) {
		this.addAll(o1);
		this.addAll(o2);
	}
}
