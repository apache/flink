package org.apache.flink.streaming.connectors.pubsub;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

/**
 * Test for {@link Bound}.
 */
public class BoundTest {
	private SourceFunction<Object> sourceFunction = mock(SourceFunction.class);

	@Test
	public void testNoShutdownBeforeCounterLimit() {
		Bound<Object> bound = Bound.boundByAmountOfMessages(10);
		bound.start(sourceFunction);
		sleep(150L);

		bound.receivedMessage();
		verifyZeroInteractions(sourceFunction);
	}

	@Test
	public void testShutdownOnCounterLimit() {
		Bound<Object> bound = Bound.boundByAmountOfMessages(3);
		bound.start(sourceFunction);

		bound.receivedMessage();
		bound.receivedMessage();
		bound.receivedMessage();

		verify(sourceFunction, times(1)).cancel();
	}

	@Test
	public void testNoShutdownBeforeTimerLimit() {
		Bound<Object> bound = Bound.boundByTimeSinceLastMessage(1000L);
		bound.start(sourceFunction);
		for (int i = 0; i < 10; i++) {
			bound.receivedMessage();
		}

		verifyZeroInteractions(sourceFunction);
	}

	@Test
	public void testShutdownAfterTimerLimitNoMessageReceived() {
		Bound<Object> bound = Bound.boundByTimeSinceLastMessage(100L);
		bound.start(sourceFunction);
		sleep(250L);
		verify(sourceFunction, times(1)).cancel();
	}

	@Test
	public void testShutdownAfterTimerLimitAfterMessageReceived() {
		Bound<Object> bound = Bound.boundByTimeSinceLastMessage(100L);
		bound.start(sourceFunction);
		sleep(50L);

		bound.receivedMessage();
		sleep(50L);
		verifyZeroInteractions(sourceFunction);

		sleep(200L);
		verify(sourceFunction, times(1)).cancel();
	}

	@Test
	public void testCounterOrTimerMaxMessages() {
		Bound<Object> bound = Bound.boundByAmountOfMessagesOrTimeSinceLastMessage(3, 1000L);
		bound.start(sourceFunction);

		bound.receivedMessage();
		bound.receivedMessage();
		bound.receivedMessage();

		verify(sourceFunction, times(1)).cancel();
	}

	@Test
	public void testCounterOrTimerTimerElapsed() {
		Bound<Object> bound = Bound.boundByAmountOfMessagesOrTimeSinceLastMessage(1L, 100L);
		bound.start(sourceFunction);
		sleep(200L);
		verify(sourceFunction, times(1)).cancel();
	}

	@Test(expected = IllegalStateException.class)
	public void testExceptionThrownIfStartNotCalled() {
		Bound<Object> bound = Bound.boundByAmountOfMessagesOrTimeSinceLastMessage(1L, 100L);
		bound.receivedMessage();
	}

	@Test(expected = IllegalStateException.class)
	public void testExceptionThrownIfStartCalledTwice() {
		Bound<Object> bound = Bound.boundByAmountOfMessagesOrTimeSinceLastMessage(1L, 100L);
		bound.start(sourceFunction);
		bound.start(sourceFunction);
	}

	private void sleep(long sleepTime) {
		try {
			Thread.sleep(sleepTime);
		} catch (InterruptedException e) {
		}
	}
}
