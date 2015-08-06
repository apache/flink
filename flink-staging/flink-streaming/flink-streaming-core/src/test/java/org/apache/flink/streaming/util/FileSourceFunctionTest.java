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

package org.apache.flink.streaming.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.streaming.api.functions.source.FileSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.runtime.tasks.StreamingRuntimeContext;
import org.apache.flink.types.IntValue;

import org.junit.Assert;
import org.junit.Test;

public class FileSourceFunctionTest {
	@Test
	public void testFileSourceFunction () {
      RuntimeContext runtimeContext =  new StreamingRuntimeContext("FileSourceTest", new MockEnvironment(3 * 1024 * 1024, new DummyInputSplitProvider(), 1024), null, new ExecutionConfig());
      
		DummyFileInputFormat inputFormat = new DummyFileInputFormat();
      inputFormat.setFilePath("file:///some/none/existing/directory/");
		FileSourceFunction<IntValue> fileSourceFunction = new FileSourceFunction<IntValue>(inputFormat, TypeExtractor.getInputFormatTypes(inputFormat));

		fileSourceFunction.setRuntimeContext(runtimeContext);
	   DummyContext<IntValue> ctx = new DummyContext<IntValue>();
		
		try {
         fileSourceFunction.open(new Configuration());
	      fileSourceFunction.run(ctx);;
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		Assert.assertTrue(ctx.getData().size() == 200);

	}
	
	@Test
	public void testFileSourceFunctionCheckpoint() {
      RuntimeContext runtimeContext =  new StreamingRuntimeContext("FileSourceTest", new MockEnvironment(3 * 1024 * 1024, new DummyInputSplitProvider(), 1024), null, new ExecutionConfig());
      
      DummyFileInputFormat inputFormat = new DummyFileInputFormat();
      inputFormat.setFilePath("file:///some/none/existing/directory/");
      FileSourceFunction<IntValue> fileSourceFunction = new FileSourceFunction<IntValue>(inputFormat, TypeExtractor.getInputFormatTypes(inputFormat));

      fileSourceFunction.setRuntimeContext(runtimeContext);
      DummyContext<IntValue> ctx = new DummyContext<IntValue>();
      
      try {
         fileSourceFunction.open(new Configuration());
         fileSourceFunction.restoreState(100l);
         fileSourceFunction.run(ctx);;
      } catch (Exception e) {
         e.printStackTrace();
      }
      
      Assert.assertTrue(ctx.getData().size() == 100);
	   
	}

	private class DummyFileInputFormat extends FileInputFormat<IntValue> {
		private static final long serialVersionUID = 1L;

		private List<Integer> data = new ArrayList<Integer>();
		private int counter = 0;

		public DummyFileInputFormat(){
			for (int i=0; i<200; i++)
				data.add(i);
		}

		@Override
		public boolean reachedEnd() throws IOException {
			return counter == data.size();
		}

		@Override
		public IntValue nextRecord(IntValue record) throws IOException {
		   if(counter + 1 > data.size()) return null;
		   else return new IntValue(data.get(counter++));
		}
	}

	private class DummyInputSplitProvider extends MockInputSplitProvider {
	   @Override
	   public InputSplit getNextInputSplit() {
	      return null;
	   }

	}
	
	private class DummyContext<IntValue> implements SourceFunction.SourceContext<IntValue> {

	   final List<Integer> data = new ArrayList<Integer>();

      @Override
      public void collect(IntValue element) {
         data.add(((org.apache.flink.types.IntValue) element).getValue());
      }

      @Override
      public Object getCheckpointLock() {
         return new Object();
      }
      
      public List<Integer> getData(){
         return data;
      }
   }

}
