package com.xusheng.flink;

import com.xusheng.flink.datasource.TransactionDatasource;
import com.xusheng.flink.processor.FraudDetector;
import com.xusheng.flink.sink.AlertSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

/**
 * @Author xusheng
 * @Date 2023/1/31 10:52
 * @Desc
 */
public class FlinkTest {

    @Test
    public void testDataSource() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Transaction> transactions = env.addSource(new TransactionDatasource())
                .name("transactions");

        DataStream<Alert> alerts = transactions.keyBy(Transaction::getAccountId)
                .process(new FraudDetector())
                .name("FraudDetector");

        alerts.addSink(new AlertSink());
        alerts.print();

        env.execute();

    }
}
