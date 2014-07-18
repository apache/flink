package org.apache.flink.api.common.functions;

/**
 * An base interface for all user-defined functions. This interface is empty in order
 * to enable functions that are SAM (single abstract method) interfaces, so that they
 * can be called as Java 8 lambdas
 */
public interface Function {

}
