package org.apache.flink.runtime.blob;

/** The type of BLOB path which indicates the location and lifecycle of BLOBs.*/
public enum BlobPathType {
    /**
     * The path is used by {@link BlobServer} in which the files are permanent.
     */
    SERVER_PERMANENT,

    /**
     * The path is used by {@link AbstractBlobCache} in which the files are transient.
     */
    CACHE_TRANSIENT,

    /**
     * The path is used by {@link AbstractBlobCache} in which the files are permanent.
     */
    CACHE_PERMANENT
}
