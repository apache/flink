/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.flink.test.recordJobs.relational.query1Util;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.MockitoAnnotations.initMocks;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.operators.shipping.RecordOutputCollector;
import org.apache.flink.test.recordJobs.util.Tuple;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Record;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

public class LineItemFilterTest {

	private static final String RETURN_FLAG = "N";
	
	@Mock
	RecordWriter<Record> recordWriterMock;
	
	private List<RecordWriter<Record>> writerList = new ArrayList<RecordWriter<Record>>();

	@Before
	public void setUp()
	{
		initMocks(this);
		writerList.add(recordWriterMock);
	}
	
	@Test
	public void shouldNotFilterTuple() throws Exception, InterruptedException
	{
		LineItemFilter out = new LineItemFilter();
		
		String shipDate = "1996-03-13";
		Tuple input = createInputTuple(shipDate);
		IntValue inputKey = new IntValue();
		Record rec = new Record();
		rec.setField(0, inputKey);
		rec.setField(1, input);
		
		Collector<Record> collector = new RecordOutputCollector(writerList);
		
		StringValue returnFlag = new StringValue(RETURN_FLAG);
		
		out.map(rec, collector);
		
		ArgumentCaptor<Record> argument = ArgumentCaptor.forClass(Record.class);
		verify(recordWriterMock).emit(argument.capture());
		assertEquals(returnFlag, argument.getValue().getField(0, StringValue.class));
		assertEquals(input, argument.getValue().getField(1, Record.class));
	}
	
	
	@Test
	public void shouldFilterTuple() throws Exception, InterruptedException
	{
		LineItemFilter out = new LineItemFilter();
		
		String shipDate = "1999-03-13";
		
		Tuple input = createInputTuple(shipDate);
		IntValue inputKey = new IntValue();
		Record rec = new Record();
		rec.setField(0, inputKey);
		rec.setField(1, input);
		
		Collector<Record> collector = new RecordOutputCollector(writerList);
		
		out.map(rec, collector);
		verifyNoMoreInteractions(recordWriterMock);
	}
	
	@Test
	public void shouldNotThrowExceptionWhenNullTuple() throws Exception
	{
		LineItemFilter out = new LineItemFilter();
		
		Tuple input = null;
		IntValue inputKey = new IntValue();
		Record rec = new Record();
		rec.setField(0, inputKey);
		rec.setField(1, input);
		
		
		Collector<Record> collector = new RecordOutputCollector(writerList);
		
		out.map(rec, collector);
		verifyNoMoreInteractions(recordWriterMock);
	}
	
	@Test
	public void shouldNoThrowExceptionOnMalformedDate() throws Exception, InterruptedException
	{
		LineItemFilter out = new LineItemFilter();
		
		String shipDate = "foobarDate";
		
		Tuple input = createInputTuple(shipDate);
		IntValue inputKey = new IntValue();
		Record rec = new Record();
		rec.setField(0, inputKey);
		rec.setField(1, input);
		
		Collector<Record> collector = new RecordOutputCollector(writerList);
		
		out.map(rec, collector);
		verifyNoMoreInteractions(recordWriterMock);
	}
	
	@Test
	public void shouldNoThrowExceptionOnTooShortTuple() throws Exception, InterruptedException
	{
		LineItemFilter out = new LineItemFilter();
		
		Tuple input = new Tuple();
		input.addAttribute("" +1);
		input.addAttribute("" + 155190);
		input.addAttribute("" + 7706);
		input.addAttribute("" + 1);
		input.addAttribute("" + 17);
		input.addAttribute("" + 21168.23);
		input.addAttribute("" + 0.04);
		input.addAttribute("" + 0.02);
		input.addAttribute(RETURN_FLAG);
		input.addAttribute("0");
		//the relevant column is missing now
		
		IntValue inputKey = new IntValue();
		Record rec = new Record();
		rec.setField(0, inputKey);
		rec.setField(1, input);
		
		Collector<Record> collector = new RecordOutputCollector(writerList);
		
		out.map(rec, collector);
		verifyNoMoreInteractions(recordWriterMock);
	}
	

	
	/**
	 * Creates a subtuple of the lineitem relation.
	 * 
	 * 1155190|7706|1|17|21168.23|0.04|0.02|N|O|1996-03-13|1996-02-12|1996-03-22|DELIVER IN PERSON|TRUCK|egular courts above the|
	 * @param shipDate the date the {@link LineItemFilter} filters for.
	 */
	private Tuple createInputTuple(String shipDate) {
		Tuple input = new Tuple();
		input.addAttribute("" +1);
		input.addAttribute("" + 155190);
		input.addAttribute("" + 7706);
		input.addAttribute("" + 1);
		input.addAttribute("" + 17);
		input.addAttribute("" + 21168.23);
		input.addAttribute("" + 0.04);
		input.addAttribute("" + 0.02);
		input.addAttribute(RETURN_FLAG);
		input.addAttribute("0");
		input.addAttribute(shipDate);
		return input;
	}
}
