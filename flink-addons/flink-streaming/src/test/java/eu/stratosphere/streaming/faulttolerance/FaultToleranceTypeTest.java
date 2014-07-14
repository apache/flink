package eu.stratosphere.streaming.faulttolerance;

import static org.junit.Assert.*;

import org.junit.Test;

public class FaultToleranceTypeTest {

	@Test
	public void test() {
		assertEquals(FaultToleranceType.NONE, FaultToleranceType.from(0));
		assertEquals(FaultToleranceType.AT_LEAST_ONCE, FaultToleranceType.from(1));
		assertEquals(FaultToleranceType.EXACTLY_ONCE, FaultToleranceType.from(2));
	}
}
