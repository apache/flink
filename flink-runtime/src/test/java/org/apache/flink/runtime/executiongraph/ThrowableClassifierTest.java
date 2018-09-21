package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.execution.SuppressRestartsException;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.throwable.ThrowableClassifier;
import org.apache.flink.runtime.throwable.ThrowableType;
import org.apache.flink.util.TestLogger;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test throwable classifier
 * */
public class ThrowableClassifierTest extends TestLogger {

	@Test
	public void testNonRecoverableFailure() {
		assertEquals(ThrowableType.NonRecoverable,
			ThrowableClassifier.getThrowableType(new SuppressRestartsException(new Exception(""))));

		assertEquals(ThrowableType.NonRecoverable,
			ThrowableClassifier.getThrowableType(new NoResourceAvailableException()));
	}
}
