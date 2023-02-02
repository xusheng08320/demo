package com.xusheng.flink;

import com.xusheng.flink.datasource.EventDataSource;
import com.xusheng.flink.datasource.TransactionDatasource;
import com.xusheng.flink.datasource.UrlVisitDataSource;
import com.xusheng.flink.dto.UrlVisitCount;
import com.xusheng.flink.processor.*;
import com.xusheng.flink.sink.AlertSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
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

    @Test
    public void testQpsByBos() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new EventDataSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps().withTimestampAssigner((event, timestamp) -> event.getTimestamp()))
                .map(event -> Tuple2.of(event.getUrl(), 1L))
                .returns(TupleTypeInfo.getBasicTupleTypeInfo(String.class, Long.class))
                .keyBy(tup -> tup.f0)
                .window(SlidingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(200)))
                .reduce((v1, v2) -> Tuple2.of(v1.f0, v1.f1 + v2.f1))
                .print();

        env.execute();

    }

    @Test
    public void testAggreFunction() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new TransactionDatasource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Transaction>forMonotonousTimestamps().withTimestampAssigner((tx, time) -> tx.getDatetime()))
                .keyBy(Transaction::getAccountId)
                .window(SlidingEventTimeWindows.of(Time.seconds(2), Time.seconds(1)))
                .aggregate(new AvgAggregateFunction())
                .print();
        env.execute();
    }

    @Test
    public void testTopN() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new UrlVisitDataSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<URLVisit>forMonotonousTimestamps().withTimestampAssigner((visit, time) -> visit.timestamp))
                .keyBy(URLVisit::getUrl)
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .aggregate(new URLVisitCountAggreFunction(), new UrlViewCountResult())
                .keyBy(UrlVisitCount::getUrl)
                .process(new TopN(2))
                .print();
        env.execute();
    }
}
