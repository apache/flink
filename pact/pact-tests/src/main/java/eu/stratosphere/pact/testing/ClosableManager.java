package eu.stratosphere.pact.testing;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Collects other {@link java.io.Closeable}s and closes them once. Instances can be used further after a call to
 * {@link #close()}.
 * 
 * @author Arvid.Heise
 */
public class ClosableManager implements Closeable {
	private List<Closeable> closeables = new ArrayList<Closeable>();

	@Override
	protected void finalize() throws Throwable {
		this.close();
		super.finalize();
	}

	@Override
	public synchronized void close() throws IOException {
		List<IOException> exceptions = null;

		for (Closeable closeable : this.closeables)
			try {
				closeable.close();
			} catch (IOException e) {
				if (exceptions == null)
					exceptions = new ArrayList<IOException>();
				exceptions.add(e);
			}
		this.closeables.clear();

		if (exceptions != null)
			throw new IOException("exception(s) while closing: " + exceptions);
	}

	/**
	 * Adds a new {@link Closeable}.
	 * 
	 * @param closeable
	 *        the closable to add
	 */
	public synchronized void add(Closeable closeable) {
		this.closeables.add(closeable);
	}
}
