package org.apache.flink.streaming.api.windowing.assigners;

import java.io.Serializable;

/**
 * @title:
 * @author: zhangyf
 * @date: 2023/7/19 13:24
 */
public interface TimeAdjustExtractor<T> extends Serializable {
    long extract(T element);
}
