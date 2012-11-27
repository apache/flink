/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

import java.util.ArrayList;

public class FieldList  extends ArrayList<Integer>{

	private static final long serialVersionUID = -6333143284628275141L;

	public FieldList() {
	}
	
	public FieldList(int columnIndex) {
		super(1);
		this.add(columnIndex);
	}
	
	public FieldList(int[] columnIndexes) {
		super(columnIndexes.length);
		for(int i=0;i<columnIndexes.length;i++) {
			if(this.contains(columnIndexes[i])) {
				throw new IllegalArgumentException("Fields must be unique");
			}
			this.add(columnIndexes[i]);
		}
	}
	
}
