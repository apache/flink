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

package eu.stratosphere.streaming.examples.iterative.collaborativefilter;


import java.util.HashMap;

import org.jblas.DoubleMatrix;

import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.streaming.api.invokable.UserTaskInvokable;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;

public class CollaborativeFilteringTask extends UserTaskInvokable {

	private static final long serialVersionUID = 1L;
	private StreamRecord outRecord = new StreamRecord(new Tuple1<String>());
	HashMap<Integer, Integer> rowIndex=new HashMap<Integer, Integer>();
	HashMap<Integer, Integer> columnIndex=new HashMap<Integer, Integer>();
	DoubleMatrix userItem=new DoubleMatrix(1000, 2000);
	DoubleMatrix coOccurence=new DoubleMatrix(2000, 2000);
	@Override
	public void invoke(StreamRecord record) throws Exception {
		// TODO Auto-generated method stub
		int userId = record.getInteger(0, 0);
		int itemId = record.getInteger(0, 1);
		int rating = record.getInteger(0, 2);
		if(!rowIndex.containsKey(userId)){
			rowIndex.put(userId, rowIndex.size());
		}
		if(!columnIndex.containsKey(itemId)){
			columnIndex.put(itemId, columnIndex.size());
		}
		userItem.put(rowIndex.get(userId), columnIndex.get(itemId), rating);
		
		//outRecord.setString(0, line);
	}

}
