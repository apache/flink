package eu.stratosphere.nephele.io.channels;

import java.nio.channels.FileChannel;

interface ChannelWithAccessInfo {

	FileChannel getChannel();

	FileChannel getAndIncrementReferences();
	
	/**
	 * Increments the references to this channel. Returns <code>true</code>, if successful, and <code>false</code>,
	 * if the channel has been disposed in the meantime.
	 * 
	 * @return True, if successful, false, if the channel has been disposed.
	 */
	boolean incrementReferences();
	
	ChannelWithPosition reserveWriteSpaceAndIncrementReferences(int spaceToReserve);
	
	/**
	 * Decrements the number of references to this channel. If the number of references is zero after the
	 * decrement, the channel is deleted.
	 * 
	 * @return The number of references remaining after the decrement.
	 * @throws IllegalStateException
	 *         Thrown, if the number of references is already zero or below.
	 */
	int decrementReferences();
	
	/**
	 * Disposes the channel without further notice. Tries to close it (swallowing all exceptions) and tries
	 * to delete the file.
	 */
	void disposeSilently();
}
