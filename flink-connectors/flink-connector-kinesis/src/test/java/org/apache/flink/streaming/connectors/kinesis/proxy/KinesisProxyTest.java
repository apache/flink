/**
 * 
 */
package org.apache.flink.streaming.connectors.kinesis.proxy;

import static org.junit.Assert.*;

import org.junit.Test;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonServiceException.ErrorType;
import com.amazonaws.services.kinesis.model.ExpiredIteratorException;
import com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException;

/**
 * Test for methods in the KinesisProxy class.
 * 
 */
public class KinesisProxyTest {

	@Test
	public void testIsRecoverableExceptionWithProvisionedThroughputExceeded() {
		final ProvisionedThroughputExceededException ex = new ProvisionedThroughputExceededException("asdf");
		ex.setErrorType(ErrorType.Client);
		assertTrue(KinesisProxy.isRecoverableException(ex));
	}

	@Test
	public void testIsRecoverableExceptionWithServiceException() {
		final AmazonServiceException ex = new AmazonServiceException("asdf");
		ex.setErrorType(ErrorType.Service);
		assertTrue(KinesisProxy.isRecoverableException(ex));
	}

	@Test
	public void testIsRecoverableExceptionWithExpiredIteratorException() {
		final ExpiredIteratorException ex = new ExpiredIteratorException("asdf");
		ex.setErrorType(ErrorType.Client);
		assertFalse(KinesisProxy.isRecoverableException(ex));
	}

	@Test
	public void testIsRecoverableExceptionWithNullErrorType() {
		final AmazonServiceException ex = new AmazonServiceException("asdf");
		ex.setErrorType(null);
		assertFalse(KinesisProxy.isRecoverableException(ex));
	}

}
