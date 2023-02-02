package com.xusheng.flink.processor;

import com.xusheng.flink.URLVisit;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @Author xusheng
 * @Date 2023/2/2 16:32
 * @Desc
 */
public class URLVisitCountAggreFunction implements AggregateFunction<URLVisit, Integer, Integer> {
    @Override
    public Integer createAccumulator() {
        return 0;
    }

    @Override
    public Integer add(URLVisit urlVisit, Integer acc) {
        return acc + 1;
    }

    @Override
    public Integer getResult(Integer acc) {
        return acc;
    }

    @Override
    public Integer merge(Integer integer, Integer acc1) {
        return null;
    }
}
