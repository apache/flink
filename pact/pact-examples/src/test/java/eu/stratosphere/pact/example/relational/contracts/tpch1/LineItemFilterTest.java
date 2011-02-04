package eu.stratosphere.pact.example.relational.contracts.tpch1;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.MockitoAnnotations.initMocks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.example.relational.util.Tuple;
import eu.stratosphere.pact.runtime.task.util.OutputCollector;

@SuppressWarnings("unchecked")
public class LineItemFilterTest {

	private static final String RETURN_FLAG = "N";
	@Mock
	RecordWriter<KeyValuePair<PactInteger, Tuple>> recordWriterMock; 
	
	@Mock
	RecordWriter<KeyValuePair<PactString, Tuple>> recordStringWriterMock; 
	
	private List<RecordWriter> writerList = new ArrayList<RecordWriter>();
	
	@Before
	public void setUp()
	{
		initMocks(this);
		writerList.add(recordStringWriterMock);
	}
	
	@Test
	public void shouldNotFilterTuple() throws IOException, InterruptedException
	{
		LineItemFilter out = new LineItemFilter();
		
		String shipDate = "1996-03-13";
		Tuple input = createInputTuple(shipDate);
		
		PactInteger inputKey = new PactInteger();
		Collector<PactString, Tuple> collector = new OutputCollector(writerList,0);
		
		PactString returnFlag = new PactString(RETURN_FLAG);
		
		out.map(inputKey, input, collector);
		verify(recordStringWriterMock).emit(new KeyValuePair<PactString, Tuple>(returnFlag, input));
	}
	
	
	@Test
	public void shouldFilterTuple() throws IOException, InterruptedException
	{
		LineItemFilter out = new LineItemFilter();
		
		String shipDate = "1999-03-13";
		
		Tuple input = createInputTuple(shipDate);
		PactInteger inputKey = new PactInteger();
		Collector<PactString, Tuple> collector = new OutputCollector(writerList,0);
		
		out.map(inputKey, input, collector);
		verifyNoMoreInteractions(recordWriterMock);
	}
	
	@Test
	public void shouldNotThrowExceptionWhenNullTuple()
	{
		LineItemFilter out = new LineItemFilter();
		
		Tuple input = null;
		PactInteger inputKey = new PactInteger();
		Collector<PactString, Tuple> collector = new OutputCollector(writerList,0);
		
		out.map(inputKey, input, collector);
		verifyNoMoreInteractions(recordWriterMock);
	}
	
	@Test
	public void shouldNoThrowExceptionOnMalformedDate() throws IOException, InterruptedException
	{
		LineItemFilter out = new LineItemFilter();
		
		String shipDate = "foobarDate";
		
		Tuple input = createInputTuple(shipDate);
		PactInteger inputKey = new PactInteger();
		Collector<PactString, Tuple> collector = new OutputCollector(writerList,0);
		
		out.map(inputKey, input, collector);
		verifyNoMoreInteractions(recordWriterMock);
	}
	
	@Test
	public void shouldNoThrowExceptionOnTooShortTuple() throws IOException, InterruptedException
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
		Collector<PactString, Tuple> collector = new OutputCollector(writerList,0);
		
		out.map(inputKey, input, collector);
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
