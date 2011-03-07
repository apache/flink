package eu.stratosphere.pact.test.histogram;

import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.type.base.PactInteger;

public class HistogramDistributionTest {
	public static class ScanMap extends MapStub<PactInteger, PactInteger, PactInteger, PactInteger> {

		@Override
		protected void map(PactInteger key, PactInteger value, Collector<PactInteger, PactInteger> out) {
		}
		
	}
}
