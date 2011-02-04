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
package eu.stratosphere.pact.runtime.task;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.when;
import static org.mockito.Matchers.*;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.Pair;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.runtime.task.TupleMock;
import eu.stratosphere.pact.runtime.task.util.KeyGroupedIterator;

/**
 * Some small stuüid test to get into testing the stubs.
 * @author Mathias Peters <mathias.peters@informatik.hu-berlin.de>
 *
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(ReduceStub.class)
public class ReduceTaskTest {

	private class MockReducer extends ReduceStub<PactInteger, TupleMock, PactInteger, TupleMock>
	{
		int reduceCount = 0;
		
		public int getReduceCount() {
			return reduceCount;
		}

		@Override
		public void reduce(PactInteger key, Iterator<TupleMock> values, Collector<PactInteger, TupleMock> out) {
			this.reduceCount++;
		}
	}
	
	private MockReducer reducer;
	@Mock
	private Iterator<Pair<PactInteger, TupleMock>> iteratorMock;
	@Mock
	private Collector<PactInteger, TupleMock> collectorMock;
	@Mock
	private KeyGroupedIterator<PactInteger, TupleMock> groupedIteratorMock;
	
	@Before
	public void setUp()
	{
		initMocks(this);
		this.reducer = new MockReducer();
	}
	
	/*
	@Test
	public void shouldNotIterateInRunWithEmptyIterator()
	{
		this.reducer.run(this.iteratorMock, collectorMock);
		assertThat(this.reducer.getReduceCount(), is(equalTo(0)));
	}
	
	@Test
	public void shouldIterateWithNullCollector()
	{
		this.reducer.run(this.iteratorMock, null);
		assertThat(this.reducer.getReduceCount(), is(equalTo(0)));
	}
	
	@Test
	public void shouldCallReduceOnes() throws Exception
	{
		whenNew(KeyGroupedIterator.class).withArguments(any()).thenReturn(this.groupedIteratorMock);
		when(groupedIteratorMock.nextKey()).thenReturn(true, false);
		this.reducer.run(this.iteratorMock, this.collectorMock);
		assertThat(this.reducer.reduceCount, is(equalTo(1)));
	}
	*/
	
}
