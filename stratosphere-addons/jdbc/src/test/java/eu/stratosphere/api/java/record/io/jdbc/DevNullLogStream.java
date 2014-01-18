package eu.stratosphere.api.java.record.io.jdbc;

import java.io.OutputStream;

public class DevNullLogStream {

	public static final OutputStream DEV_NULL = new OutputStream() {
        public void write(int b) {}
    };
	
}
