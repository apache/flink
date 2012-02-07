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

package eu.stratosphere.pact.test.testPrograms.tpch1;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.MockitoAnnotations.initMocks;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.runtime.task.util.OutputCollector;
import eu.stratosphere.pact.test.testPrograms.util.Tuple;

public class LineItemFilterTest {

	private static final String RETURN_FLAG = "N";
	
	@Mock
	RecordWriter<PactRecord> recordWriterMock; 
	
	private List<RecordWriter<PactRecord>> writerList = new ArrayList<RecordWriter<PactRecord>>();
	
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
		PactInteger inputKey = new PactInteger();
		PactRecord rec = new PactRecord();
		rec.setField(0, inputKey);
		rec.setField(1, input);
		
		Collector collector = new OutputCollector(writerList);
		
		PactString returnFlag = new PactString(RETURN_FLAG);
		
		out.map(rec, collector);
		
		ArgumentCaptor<PactRecord> argument = ArgumentCaptor.forClass(PactRecord.class);
		verify(recordWriterMock).emit(argument.capture());
		assertEquals(returnFlag, argument.getValue().getField(0, PactString.class));
		assertEquals(input, argument.getValue().getField(1, PactRecord.class));
	}
	
	
	@Test
	public void shouldFilterTuple() throws Exception, InterruptedException
	{
		LineItemFilter out = new LineItemFilter();
		
		String shipDate = "1999-03-13";
		
		Tuple input = createInputTuple(shipDate);
		PactInteger inputKey = new PactInteger();
		PactRecord rec = new PactRecord();
		rec.setField(0, inputKey);
		rec.setField(1, input);
		
		Collector collector = new OutputCollector(writerList);
		
		out.map(rec, collector);
		verifyNoMoreInteractions(recordWriterMock);
	}
	
	@Test
	public void shouldNotThrowExceptionWhenNullTuple() throws Exception
	{
		LineItemFilter out = new LineItemFilter();
		
		Tuple input = null;
		PactInteger inputKey = new PactInteger();
		PactRecord rec = new PactRecord();
		rec.setField(0, inputKey);
		rec.setField(1, input);
		
		
		Collector collector = new OutputCollector(writerList);
		
		out.map(rec, collector);
		verifyNoMoreInteractions(recordWriterMock);
	}
	
	@Test
	public void shouldNoThrowExceptionOnMalformedDate() throws Exception, InterruptedException
	{
		LineItemFilter out = new LineItemFilter();
		
		String shipDate = "foobarDate";
		
		Tuple input = createInputTuple(shipDate);
		PactInteger inputKey = new PactInteger();
		PactRecord rec = new PactRecord();
		rec.setField(0, inputKey);
		rec.setField(1, input);
		
		Collector collector = new OutputCollector(writerList);
		
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
		
		PactInteger inputKey = new PactInteger();
		PactRecord rec = new PactRecord();
		rec.setField(0, inputKey);
		rec.setField(1, input);
		
		Collector collector = new OutputCollector(writerList);
		
		out.map(rec, collector);
		verifyNoMoreInteractions(recordWriterMock);
	}
	

	
	/**
	 * Creates a subtuple of the lineitem relation.
	 * 
	 * 1155190|7706|1|17|21168.23|0.04|0.02|N|O|1996-03-13|1996-02-12|1996-03-22|DELIVER IN PERSON|TRUCK|egular courts above the|
	 * @param shipDate the date the {@link LineItemFilter} filters for.
	 * @return
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
