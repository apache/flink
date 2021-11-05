package org.apache.flink.fs.gs;

import org.apache.flink.fs.gs.storage.GSBlobIdentifier;

/** Constants for testing purposes. */
public class TestUtils {

    public static final long RANDOM_SEED = 27;

    public static final byte BYTE_VALUE = 123;

    /* A commonly used blob identifier. */
    public static final GSBlobIdentifier BLOB_IDENTIFIER = new GSBlobIdentifier("foo", "bar");
}
