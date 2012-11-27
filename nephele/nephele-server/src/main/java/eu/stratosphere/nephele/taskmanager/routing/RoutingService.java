package eu.stratosphere.nephele.taskmanager.routing;

import java.io.IOException;

import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;

public interface RoutingService {

	/**
	 * Routes a {@link TransferEnvelope} from an output channel to its destination. The method may block until the
	 * receiver of the envelope is able to accept it.
	 * 
	 * @param transferEnvelope
	 *        the transfer envelope to be routed
	 * @throws IOException
	 *         thrown if an IO error occurs while routing the envelope towards its destination
	 * @throws InterruptedException
	 *         thrown if the calling thread is interrupted while waiting for the receiver to accept the envelope
	 */
	void routeEnvelopeFromOutputChannel(final TransferEnvelope transferEnvelope) throws IOException,
			InterruptedException;

	/**
	 * Routes a {@link TransferEnvelope} from an input channel to its destination. The method may block until the
	 * receiver of the envelope is able to accept it.
	 * 
	 * @param transferEnvelope
	 *        the transfer envelope to be routed
	 * @throws IOException
	 *         thrown if an IO error occurs while routing the envelope towards its destination
	 * @throws InterruptedException
	 *         thrown if the calling thread is interrupted while waiting for the receiver to accept the envelope
	 */
	void routeEnvelopeFromInputChannel(final TransferEnvelope transferEnvelope) throws IOException,
			InterruptedException;

	/**
	 * Routes a {@link TransferEnvelope} from the network service to its destination. The method may block until the
	 * receiver of the envelope is able to accept it.
	 * 
	 * @param transferEnvelope
	 *        the transfer envelope to be routed
	 * @throws IOException
	 *         thrown if an IO error occurs while routing the envelope towards its destination
	 * @throws InterruptedException
	 *         thrown if the calling thread is interrupted while waiting for the receiver to accept the envelope
	 */
	void routeEnvelopeFromNetwork(final TransferEnvelope transferEnvelope, boolean freeSourceBuffer)
			throws IOException, InterruptedException;
}
