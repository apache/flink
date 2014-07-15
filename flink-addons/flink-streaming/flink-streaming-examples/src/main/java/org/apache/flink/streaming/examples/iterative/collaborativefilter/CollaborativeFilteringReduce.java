/**
 *
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
 *
 */

package org.apache.flink.streaming.examples.iterative.collaborativefilter;

//public class CollaborativeFilteringReduce extends UserTaskInvokable {
//
//	private static final long serialVersionUID = 1L;
//	private StreamRecord outRecord = new StreamRecord(new Tuple1<String>());
//	HashMap<Integer, Integer> rowIndex=new HashMap<Integer, Integer>();
//	HashMap<Integer, Integer> columnIndex=new HashMap<Integer, Integer>();
//	DoubleMatrix userItem=new DoubleMatrix(1000, 2000);
//	DoubleMatrix coOccurence=new DoubleMatrix(2000, 2000);
//	@Override
//	public void invoke(StreamRecord record) throws Exception {
//		int userId = record.getInteger(0, 0);
//		int itemId = record.getInteger(0, 1);
//		int rating = record.getInteger(0, 2);
//		if(!rowIndex.containsKey(userId)){
//			rowIndex.put(userId, rowIndex.size());
//		}
//		if(!columnIndex.containsKey(itemId)){
//			columnIndex.put(itemId, columnIndex.size());
//		}
//		userItem.put(rowIndex.get(userId), columnIndex.get(itemId), rating);
//		
//		//outRecord.setString(0, line);
//	}
//
// }
