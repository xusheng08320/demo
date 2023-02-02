package com.xusheng.flink.datasource;

import com.xusheng.flink.Transaction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.TimeUnit;

/**
 * @Author xusheng
 * @Date 2023/1/31 13:39
 * @Desc
 */
public class TransactionDatasource implements SourceFunction<Transaction> {
    @Override
    public void run(SourceContext<Transaction> sourceContext) throws Exception {
        Transaction tx1 = new Transaction(1L,123L, 10D, 1000L);
        Transaction tx2 = new Transaction(2L, 123L,20D, 1500L);
        Transaction tx3 = new Transaction(3L, 123L,30D, 2000L);
        Transaction tx4 = new Transaction(4L, 123L,40D, 2500L);
        Transaction tx5 = new Transaction(5L, 123L,50D, 3000L);
        Transaction tx6 = new Transaction(6L, 123L,60D, 3500L);
        Transaction tx7 = new Transaction(7L, 123L,70D, 4000L);
        Transaction tx8 = new Transaction(8L, 123L,80D, 4500L);
        Transaction tx9 = new Transaction(9L, 123L,90D, 5000L);
        Transaction tx10 = new Transaction(10L,123L,100D, 5500L);

        sourceContext.collect(tx1);
        sourceContext.collect(tx2);
        sourceContext.collect(tx3);
        //Thread.sleep(TimeUnit.SECONDS.toMillis(59));
        sourceContext.collect(tx4);
        sourceContext.collect(tx5);
        sourceContext.collect(tx6);
        sourceContext.collect(tx7);
        sourceContext.collect(tx8);
        sourceContext.collect(tx9);
        sourceContext.collect(tx10);
    }

    @Override
    public void cancel() {

    }
}
