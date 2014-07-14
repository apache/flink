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

package eu.stratosphere.streaming.api.state;

import org.junit.Test;

import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.streaming.state.LogTableState;
import eu.stratosphere.streaming.state.MutableTableState;
import eu.stratosphere.streaming.state.TableStateIterator;
import eu.stratosphere.streaming.state.WindowState;

public class InternalStateTest {
	
	@Test
	public void MutableTableStateTest(){
		MutableTableState<String, String> state=new MutableTableState<String, String>();
		state.put("abc", "hello");
		state.put("test", "world");
		state.put("state", "mutable");
		state.put("streaming", "persist");
		String s=state.get("streaming");
		if(s==null){
			System.out.println("key does not exist!");
		}
		else{
			System.out.println("value="+s);
		}
		s=state.get("null");
		if(s==null){
			System.out.println("key does not exist!");
		}
		else{
			System.out.println("value="+s);
		}
		TableStateIterator<String, String> iterator=state.getIterator();
		while(iterator.hasNext()){
			Tuple2<String, String> tuple=iterator.next();
			System.out.println(tuple.getField(0)+", "+tuple.getField(1));
		}
	}
	
	@Test
	public void LogTableStateTest(){
		LogTableState<String, String> state=new LogTableState<String, String>();
		state.put("abc", "hello");
		state.put("test", "world");
		state.put("state", "mutable");
		state.put("streaming", "persist");
		String s=state.get("streaming");
		if(s==null){
			System.out.println("key does not exist!");
		}
		else{
			System.out.println("value="+s);
		}
		s=state.get("null");
		if(s==null){
			System.out.println("key does not exist!");
		}
		else{
			System.out.println("value="+s);
		}
		TableStateIterator<String, String> iterator=state.getIterator();
		while(iterator.hasNext()){
			Tuple2<String, String> tuple=iterator.next();
			System.out.println(tuple.getField(0)+", "+tuple.getField(1));
		}
	}
	
	@Test
	public void WindowStateTest(){
		WindowState<String> state=new WindowState<String>(100, 20, 10, 2);
		
	}
}
