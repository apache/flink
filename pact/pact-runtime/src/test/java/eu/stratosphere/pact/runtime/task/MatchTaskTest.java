package eu.stratosphere.pact.runtime.task;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MatchStub;
import eu.stratosphere.pact.common.type.Pair;
import eu.stratosphere.pact.common.type.base.PactInteger;

/**
 * 
 * @author Mathias Peters <mathias.peters@informatik.hu-berlin.de>
 *
 */
public class MatchTaskTest {

	private class MockMatchStub extends MatchStub<PactInteger, TupleMock, TupleMock, PactInteger, TupleMock>
	{
		private int matchCount = 0;
		
		//this could be done a lot more elegant with a true Pair type
		private List<List<TupleMock>> matchPairs = new ArrayList<List<TupleMock>>();
		
		/**
		 * Just count the number of calls and the tuple combinations
		 */
		@Override
		public void match(PactInteger key, TupleMock value1, TupleMock value2, Collector<PactInteger, TupleMock> out) {
			this.matchCount++;
			List<TupleMock> pair = new ArrayList<TupleMock>();
			pair.add(value1);
			pair.add(value2);
			this.matchPairs.add(pair);
		}
	}
	
	@Mock
	private Iterator<TupleMock> valueIteratorMock1;
	@Mock
	private Iterator<TupleMock> valueIteratorMock2;
	@Mock
	private Collector<PactInteger, TupleMock> collectorMock;
	
	private TupleMock leftValue;
	private TupleMock leftValue2;
	private TupleMock leftValue3;
	private TupleMock leftValue4;
	
	
	private TupleMock rightValue;
	private TupleMock rightValue2;
	private TupleMock rightValue3;
	private TupleMock rightValue4;
	
	
	private MockMatchStub matcher;
	
	private MatchTask matchTask;
	
	@Before
	public void setUp()
	{
		initMocks(this);
		this.matcher = new MockMatchStub();
		this.leftValue = new TupleMock("l1");
		this.leftValue2 = new TupleMock("l2");
		this.leftValue3 = new TupleMock("l3");
		this.leftValue4 = new TupleMock("l4");
		this.rightValue = new TupleMock("r1");
		this.rightValue2 = new TupleMock("r2");
		this.rightValue3 = new TupleMock("r3");
		this.rightValue4 = new TupleMock("r4");
		
		matchTask = new MatchTask();
		
	}

	/*
	@Test
	public void shouldNotRunMatch()
	{
		this.matcher.run(null, valueIteratorMock1, valueIteratorMock2, collectorMock);
		assertThat(this.matcher.matchCount, is(equalTo(0)));
		
		when(valueIteratorMock1.next()).thenReturn(leftValue);
		when(valueIteratorMock2.next()).thenReturn(null);
		this.matcher.run(null, valueIteratorMock1, valueIteratorMock2, collectorMock);
		assertThat(this.matcher.matchCount, is(equalTo(0)));
		
		when(valueIteratorMock1.next()).thenReturn(null);
		when(valueIteratorMock2.next()).thenReturn(rightValue);
		this.matcher.run(null, valueIteratorMock1, valueIteratorMock2, collectorMock);
		assertThat(this.matcher.matchCount, is(equalTo(0)));
	}
	
	@Test
	public void shouldRunMatchOnce()
	{
		when(valueIteratorMock1.next()).thenReturn(leftValue);
		when(valueIteratorMock2.next()).thenReturn(rightValue);
		this.matcher.run(null, valueIteratorMock1, valueIteratorMock2, collectorMock);
		assertThat(this.matcher.matchCount, is(equalTo(1)));
		assertThat(this.matcher.matchPairs.size(), is(equalTo(1)));
		
		List<TupleMock> onlyPair = this.matcher.matchPairs.get(0);
		assertThat(onlyPair.get(0), is(equalTo(leftValue)));
		assertThat(onlyPair.get(1), is(equalTo(rightValue)));
	}
	
	@Test
	public void shouldMatchOneLeftValueWithMultipleRightValues()
	{
		when(valueIteratorMock1.next()).thenReturn(leftValue);
		when(valueIteratorMock2.next()).thenReturn(rightValue, rightValue2, rightValue3, rightValue4);
		when(valueIteratorMock2.hasNext()).thenReturn(true, true, true, false);
		this.matcher.run(null, valueIteratorMock1, valueIteratorMock2, collectorMock);
		assertThat(this.matcher.matchCount, is(equalTo(4)));
		assertThat(this.matcher.matchPairs.size(), is(equalTo(4)));
		
		List<TupleMock> firstPair = this.matcher.matchPairs.get(0);
		assertThat(firstPair.get(0), is(equalTo(leftValue)));
		assertThat(firstPair.get(1), is(equalTo(rightValue)));
		
		List<TupleMock> secondPair = this.matcher.matchPairs.get(1);
		assertThat(secondPair.get(0), is(equalTo(leftValue)));
		assertThat(secondPair.get(1), is(equalTo(rightValue2)));
		
		List<TupleMock> thirdPair = this.matcher.matchPairs.get(2);
		assertThat(thirdPair.get(0), is(equalTo(leftValue)));
		assertThat(thirdPair.get(1), is(equalTo(rightValue3)));
		
		List<TupleMock> fourthPair = this.matcher.matchPairs.get(3);
		assertThat(fourthPair.get(0), is(equalTo(leftValue)));
		assertThat(fourthPair.get(1), is(equalTo(rightValue4)));
		
	}
	
	@Test
	public void shouldMatchOneRighValueWithMultipleLeftValues()
	{
		when(valueIteratorMock1.hasNext()).thenReturn(true, true, true, false);
		when(valueIteratorMock1.next()).thenReturn(leftValue, leftValue2, leftValue3, leftValue4);
		when(valueIteratorMock2.next()).thenReturn(rightValue);
		
		this.matcher.run(null, valueIteratorMock1, valueIteratorMock2, collectorMock);
		assertThat(this.matcher.matchCount, is(equalTo(4)));
		assertThat(this.matcher.matchPairs.size(), is(equalTo(4)));
		
		List<TupleMock> firstPair = this.matcher.matchPairs.get(0);
		assertThat(firstPair.get(0), is(equalTo(leftValue)));
		assertThat(firstPair.get(1), is(equalTo(rightValue)));
		
		List<TupleMock> secondPair = this.matcher.matchPairs.get(1);
		assertThat(secondPair.get(0), is(equalTo(leftValue2)));
		assertThat(secondPair.get(1), is(equalTo(rightValue)));
		
		List<TupleMock> thirdPair = this.matcher.matchPairs.get(2);
		assertThat(thirdPair.get(0), is(equalTo(leftValue3)));
		assertThat(thirdPair.get(1), is(equalTo(rightValue)));
		
		List<TupleMock> fourthPair = this.matcher.matchPairs.get(3);
		assertThat(fourthPair.get(0), is(equalTo(leftValue4)));
		assertThat(fourthPair.get(1), is(equalTo(rightValue)));
	}
	
	
	@Test
	public void shouldMatchMultipleRighValuesWithMultipleLeftValues()
	{
		when(valueIteratorMock1.hasNext()).thenReturn(true, true, true, true, false);
		when(valueIteratorMock1.next()).thenReturn(leftValue, leftValue2, leftValue3, leftValue4);
		when(valueIteratorMock2.hasNext()).thenReturn(true, true, true, true, false);
		when(valueIteratorMock2.next()).thenReturn(rightValue, rightValue2, rightValue3, rightValue4);
		
		this.matcher.run(null, valueIteratorMock1, valueIteratorMock2, collectorMock);
		assertThat(this.matcher.matchCount, is(equalTo(16)));
		assertThat(this.matcher.matchPairs.size(), is(equalTo(16)));
		
		List<TupleMock> firstPair = this.matcher.matchPairs.get(0);
		assertThat(firstPair.get(0), is(equalTo(leftValue)));
		assertThat(firstPair.get(1), is(equalTo(rightValue)));
		
		List<TupleMock> secondPair = this.matcher.matchPairs.get(1);
		assertThat(secondPair.get(0), is(equalTo(leftValue2)));
		assertThat(secondPair.get(1), is(equalTo(rightValue)));
		
		List<TupleMock> thirdPair = this.matcher.matchPairs.get(2);
		assertThat(thirdPair.get(0), is(equalTo(leftValue3)));
		assertThat(thirdPair.get(1), is(equalTo(rightValue)));
		
		List<TupleMock> fourthPair = this.matcher.matchPairs.get(3);
		assertThat(fourthPair.get(0), is(equalTo(leftValue4)));
		assertThat(fourthPair.get(1), is(equalTo(rightValue)));
		
		List<TupleMock> fifthPair = this.matcher.matchPairs.get(4);
		assertThat(fifthPair.get(0), is(equalTo(leftValue)));
		assertThat(fifthPair.get(1), is(equalTo(rightValue2)));
		
		List<TupleMock> sixthPair = this.matcher.matchPairs.get(5);
		assertThat(sixthPair.get(0), is(equalTo(leftValue2)));
		assertThat(sixthPair.get(1), is(equalTo(rightValue2)));
		
		List<TupleMock> seventhPair = this.matcher.matchPairs.get(6);
		assertThat(seventhPair.get(0), is(equalTo(leftValue3)));
		assertThat(seventhPair.get(1), is(equalTo(rightValue2)));
		
		List<TupleMock> eighthPair = this.matcher.matchPairs.get(7);
		assertThat(eighthPair.get(0), is(equalTo(leftValue4)));
		assertThat(eighthPair.get(1), is(equalTo(rightValue2)));
		
		List<TupleMock> ninethPair = this.matcher.matchPairs.get(8);
		assertThat(ninethPair.get(0), is(equalTo(leftValue)));
		assertThat(ninethPair.get(1), is(equalTo(rightValue3)));
		
		List<TupleMock> tenthPair = this.matcher.matchPairs.get(9);
		assertThat(tenthPair.get(0), is(equalTo(leftValue2)));
		assertThat(tenthPair.get(1), is(equalTo(rightValue3)));
		
		List<TupleMock> eleventhPair = this.matcher.matchPairs.get(10);
		assertThat(eleventhPair.get(0), is(equalTo(leftValue3)));
		assertThat(eleventhPair.get(1), is(equalTo(rightValue3)));
		
		List<TupleMock> twelvethPair = this.matcher.matchPairs.get(11);
		assertThat(twelvethPair.get(0), is(equalTo(leftValue4)));
		assertThat(twelvethPair.get(1), is(equalTo(rightValue3)));
		
		List<TupleMock> thirteenthPair = this.matcher.matchPairs.get(12);
		assertThat(thirteenthPair.get(0), is(equalTo(leftValue)));
		assertThat(thirteenthPair.get(1), is(equalTo(rightValue4)));
		
		List<TupleMock> fourteenthPair = this.matcher.matchPairs.get(13);
		assertThat(fourteenthPair.get(0), is(equalTo(leftValue2)));
		assertThat(fourteenthPair.get(1), is(equalTo(rightValue4)));
		
		List<TupleMock> fifteenthPair = this.matcher.matchPairs.get(14);
		assertThat(fifteenthPair.get(0), is(equalTo(leftValue3)));
		assertThat(fifteenthPair.get(1), is(equalTo(rightValue4)));
		
		List<TupleMock> sixteenthPair = this.matcher.matchPairs.get(15);
		assertThat(sixteenthPair.get(0), is(equalTo(leftValue4)));
		assertThat(sixteenthPair.get(1), is(equalTo(rightValue4)));
	}
	*/
	
	
}
