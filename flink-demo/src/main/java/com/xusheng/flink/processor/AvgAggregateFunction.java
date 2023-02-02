package com.xusheng.flink.processor;

import com.xusheng.flink.Transaction;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @Author xusheng
 * @Date 2023/2/1 15:55
 * @Desc
 */
public class AvgAggregateFunction implements AggregateFunction<Transaction, Tuple2<Double, Long>, Double> {
    @Override
    public Tuple2<Double, Long> createAccumulator() {
        return Tuple2.of(0D, 0L);
    }

    @Override
    public Tuple2<Double, Long> add(Transaction transaction, Tuple2<Double, Long> accumulator) {
        return Tuple2.of(accumulator.f0 + transaction.amount, accumulator.f1 + 1);
    }

    @Override
    public Double getResult(Tuple2<Double, Long> accumulator) {
        return accumulator.f0 / accumulator.f1;
    }

    @Override
    public Tuple2<Double, Long> merge(Tuple2<Double, Long> a, Tuple2<Double, Long> b) {
        //return Tuple2.of(a.f0 + b.f0, a.f1 + a.f1);
        return null;
    }
}
