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

	/**
	 * Updates the flag which indicates whether the underlying physical file shall be deleted when it is closed. Once
	 * the flag was updated to <code>false</code> it cannot be set to <code>true</code> again.
	 * 
	 * @param deleteOnClose
	 *        <code>true</code> to indicate the file shall be deleted when closed, <code>false</code> otherwise
	 */
	void updateDeleteOnCloseFlag(final boolean deleteOnClose);
}
