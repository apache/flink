package eu.stratosphere.pact.testing;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ClosableManager implements Closeable {
	private List<Closeable> closeables = new ArrayList<Closeable>();

	@Override
	protected void finalize() throws Throwable {
		close();
		super.finalize();
	}

	@Override
	public synchronized void close() throws IOException {
		List<IOException> exceptions = null;

		for (Closeable closeable : closeables) {
			try {
				closeable.close();
			} catch (IOException e) {
				if (exceptions == null)
					exceptions = new ArrayList<IOException>();
				exceptions.add(e);
			}
		}
		closeables.clear();

		if (exceptions != null)
			throw new IOException("exception(s) while closing: " + exceptions);
	}

	public synchronized void add(Closeable closeable) {
		closeables.add(closeable);
	}
}
