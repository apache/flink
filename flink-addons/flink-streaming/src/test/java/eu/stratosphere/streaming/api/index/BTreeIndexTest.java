/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.streaming.api.index;

import org.junit.Test;

import eu.stratosphere.streaming.index.BTreeIndex;
import eu.stratosphere.streaming.index.IndexPair;

public class BTreeIndexTest {
	
	@Test
	public void bTreeIndexOperationTest(){
		BTreeIndex<String, IndexPair> btree=new BTreeIndex<String, IndexPair>();
		btree.put("abc", new IndexPair(7, 3));
		btree.put("abc", new IndexPair(1, 2));
		btree.put("def", new IndexPair(6, 3));
		btree.put("ghi", new IndexPair(3, 6));
		btree.put("jkl", new IndexPair(4, 7));
		System.out.println(btree.get("abc").blockId+", "+btree.get("abc").entryId);
	}
}
