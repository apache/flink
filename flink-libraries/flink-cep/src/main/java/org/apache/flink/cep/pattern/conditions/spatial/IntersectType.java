package org.apache.flink.cep.pattern.conditions.spatial;

/** Supported intersection types. */
public enum IntersectType {
    INTERSECT_ALL,
    INTERSECT_ANY_N,
    INTERSECT_EXACTLY_N,
    INTERSECT_INORDER_N,
    INTERSECT_INORDER_EXACTLY_N,
}
