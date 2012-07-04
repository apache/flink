package eu.stratosphere.pact.common.plan;

import static org.junit.Assert.assertEquals;

import java.util.Iterator;

import org.junit.Test;

import eu.stratosphere.pact.common.contract.CoGroupContract;
import eu.stratosphere.pact.common.contract.CrossContract;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.io.DelimitedInputFormat;
import eu.stratosphere.pact.common.io.DelimitedOutputFormat;
import eu.stratosphere.pact.common.stubs.CoGroupStub;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.CrossStub;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.stubs.Stub;
import eu.stratosphere.pact.common.type.PactRecord;

/**
 * Tests {@link ContractUtil}.
 * 
 * @author Arvid Heise
 */
public class ContractUtilTest {
	/**
	 * Test {@link ContractUtil#getContractClass(Class)}
	 */
	@Test
	public void getContractClassShouldReturnCoGroupForCoGroupStub() {
		final Class<?> result = ContractUtil.getContractClass(CoGrouper.class);
		assertEquals(CoGroupContract.class, result);
	}

	/**
	 * Test {@link ContractUtil#getContractClass(Class)}
	 */
	@Test
	public void getContractClassShouldReturnCrossForCrossStub() {
		final Class<?> result = ContractUtil.getContractClass(Crosser.class);
		assertEquals(CrossContract.class, result);
	}

	/**
	 * Test {@link ContractUtil#getContractClass(Class)}
	 */
	@Test
	public void getContractClassShouldReturnMapForMapStub() {
		final Class<?> result = ContractUtil.getContractClass(Mapper.class);
		assertEquals(MapContract.class, result);
	}

	/**
	 * Test {@link ContractUtil#getContractClass(Class)}
	 */
	@Test
	public void getContractClassShouldReturnMatchForMatchStub() {
		final Class<?> result = ContractUtil.getContractClass(Matcher.class);
		assertEquals(MatchContract.class, result);
	}

	/**
	 * Test {@link ContractUtil#getContractClass(Class)}
	 */
	@Test
	public void getContractClassShouldReturnNullForStub() {
		final Class<?> result = ContractUtil.getContractClass(Stub.class);
		assertEquals(null, result);
	}

	/**
	 * Test {@link ContractUtil#getContractClass(Class)}
	 */
	@Test
	public void getContractClassShouldReturnReduceForReduceStub() {
		final Class<?> result = ContractUtil.getContractClass(Reducer.class);
		assertEquals(ReduceContract.class, result);
	}

	/**
	 * Test {@link ContractUtil#getContractClass(Class)}
	 */
	@Test
	public void getContractClassShouldReturnSinkForOutputFormat() {
		final Class<?> result = ContractUtil.getContractClass(DelimitedOutputFormat.class);
		assertEquals(FileDataSink.class, result);
	}

	/**
	 * Test {@link ContractUtil#getContractClass(Class)}
	 */
	@Test
	public void getContractClassShouldReturnSourceForInputFormat() {
		final Class<?> result = ContractUtil.getContractClass(DelimitedInputFormat.class);
		assertEquals(FileDataSource.class, result);
	}

	static class CoGrouper extends CoGroupStub {
		/* (non-Javadoc)
		 * @see eu.stratosphere.pact.common.stubs.CoGroupStub#coGroup(java.util.Iterator, java.util.Iterator, eu.stratosphere.pact.common.stubs.Collector)
		 */
		@Override
		public void coGroup(Iterator<PactRecord> records1, Iterator<PactRecord> records2, Collector<PactRecord> out) {
		}
	}

	static class Crosser extends CrossStub {
		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.pact.common.stubs.CrossStub#cross(eu.stratosphere.pact.common.type.PactRecord,
		 * eu.stratosphere.pact.common.type.PactRecord, eu.stratosphere.pact.common.stubs.Collector)
		 */
		@Override
		public void cross(PactRecord record1, PactRecord record2, Collector<PactRecord> out) {
		}
	}

	static class Mapper extends MapStub {
		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.pact.common.stubs.MapStub#map(eu.stratosphere.pact.common.type.PactRecord,
		 * eu.stratosphere.pact.common.stubs.Collector)
		 */
		@Override
		public void map(PactRecord record, Collector<PactRecord> out) throws Exception {
		}
	}

	static class Matcher extends MatchStub {
		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.pact.common.stubs.MatchStub#match(eu.stratosphere.pact.common.type.PactRecord,
		 * eu.stratosphere.pact.common.type.PactRecord, eu.stratosphere.pact.common.stubs.Collector)
		 */
		@Override
		public void match(PactRecord value1, PactRecord value2, Collector<PactRecord> out) throws Exception {
		}
	}

	static class Reducer extends ReduceStub {
		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.pact.common.stubs.ReduceStub#reduce(java.util.Iterator,
		 * eu.stratosphere.pact.common.stubs.Collector)
		 */
		@Override
		public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out) throws Exception {
		}
	}
}
