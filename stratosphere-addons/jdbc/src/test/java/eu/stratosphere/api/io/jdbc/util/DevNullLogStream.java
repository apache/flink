package eu.stratosphere.api.io.jdbc.util;

import java.io.OutputStream;

public class DevNullLogStream {

	public static final OutputStream DEV_NULL = new OutputStream() {
        public void write(int b) {}
    };
	
}
