package eu.stratosphere.nephele.services.iomanager;


import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;


/**
 *
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public final class RequestQueue<E> extends LinkedBlockingQueue<E> implements Closeable
{
	/**
	 * UID for serialization interoperability. 
	 */
	private static final long serialVersionUID = 3804115535778471680L;
	
	/**
	 * Flag marking this queue as closed.
	 */
	private volatile boolean closed = false;
	
	/**
	 * Closes this request queue.
	 * 
	 * @see java.io.Closeable#close()
	 */
	@Override
	public void close() throws IOException {
		this.closed = true;
	}
	
	/**
	 * Checks whether this request queue is closed.
	 * 
	 * @return True, if the queue is closed, false otherwise.
	 */
	public boolean isClosed()
	{
		return this.closed;
	}
	
}
