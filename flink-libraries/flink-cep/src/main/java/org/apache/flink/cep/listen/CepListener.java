package org.apache.flink.cep.listen;

import org.apache.flink.cep.pattern.Pattern;

import java.io.Serializable;

/**
 * @title:
 * @author: zhangyf
 * @date: 2023/7/19 15:50
 */
public interface CepListener<T> extends Serializable {

    /**
     * @Description: 留给用户判断当接受到元素的时候，是否需要更新CEP逻辑
     *
     * @param element
     * @return java.lang.Boolean
     */
    Boolean needChange(T element);

    /**
     * @Description: 当needChange为true时会被调用，留给用户实现返回一个新逻辑生成的pattern对象
     *
     * @param flagElement
     * @return org.apache.flink.cep.pattern.Pattern
     */
    Pattern<T, ?> returnPattern(T flagElement);
}
