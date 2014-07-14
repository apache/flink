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

package eu.stratosphere.streaming.state.manager;

import java.util.LinkedList;

import eu.stratosphere.streaming.state.TableState;

public class StateCheckpointer {

	private LinkedList<TableState> stateList = new LinkedList<TableState>();
	
	public void RegisterState(TableState state){
		stateList.add(state);
	}
	
	public void CheckpointStates(){
		for(TableState state: stateList){
			//take snapshot of every registered state.
			
		}		
	}
}
