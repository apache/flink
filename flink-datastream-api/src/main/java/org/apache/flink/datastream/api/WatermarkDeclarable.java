package org.apache.flink.datastream.api;

import org.apache.flink.api.common.WatermarkDeclaration;

import java.util.Collections;
import java.util.Set;

/** Interface to expose {@link WatermarkDeclaration}s. */
public interface WatermarkDeclarable {

    /** Provide a Set of {@link WatermarkDeclaration} implementations. */
    default Set<Class<? extends WatermarkDeclaration>> watermarkDeclarations() {
        return Collections.emptySet();
    }
}
