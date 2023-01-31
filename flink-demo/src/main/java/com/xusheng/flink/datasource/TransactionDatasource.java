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
        Transaction tx1 = new Transaction(1L,123L, 13.01D);
        Transaction tx2 = new Transaction(2L, 123L,25D);
        Transaction tx3 = new Transaction(3L, 123L,0.09D);
        Transaction tx4 = new Transaction(4L, 123L,510D);
        Transaction tx5 = new Transaction(5L, 123L,102.62D);
        Transaction tx6 = new Transaction(6L, 123L,91.5D);
        Transaction tx7 = new Transaction(7L, 123L,0.02D);
        Transaction tx8 = new Transaction(8L, 123L,30.01D);
        Transaction tx9 = new Transaction(9L, 123L,701.83D);
        Transaction tx10 = new Transaction(10L,123L,31.92D);

        sourceContext.collect(tx1);
        sourceContext.collect(tx2);
        sourceContext.collect(tx3);
        Thread.sleep(TimeUnit.SECONDS.toMillis(59));
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
