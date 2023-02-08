package com.xusheng.flink;

import com.xusheng.flink.datasource.EventDataSource;
import com.xusheng.flink.datasource.TransactionDatasource;
import com.xusheng.flink.datasource.UrlVisitDataSource;
import com.xusheng.flink.dto.UrlVisitCount;
import com.xusheng.flink.processor.*;
import com.xusheng.flink.sink.AlertSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.server.quorum.Leader;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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

    @Test
    public void testSideOutPut() throws Exception {
        OutputTag<Tuple2<String, Long>> indexTag = new OutputTag<Tuple2<String, Long>>("index-tag"){};
        OutputTag<Tuple2<String, Long>> cartTag = new OutputTag<Tuple2<String, Long>>("cart-tag"){};
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<URLVisit> processStream = env.addSource(new UrlVisitDataSource())
                .process(new ProcessFunction<URLVisit, URLVisit>() {
                    @Override
                    public void processElement(URLVisit urlVisit, ProcessFunction<URLVisit, URLVisit>.Context context, Collector<URLVisit> collector) throws Exception {
                        if ("/index".equals(urlVisit.getUrl())) {
                            context.output(indexTag, Tuple2.of(urlVisit.url, urlVisit.timestamp));
                        } else if ("/cart".equals(urlVisit.getUrl())) {
                            context.output(cartTag, Tuple2.of(urlVisit.url, urlVisit.timestamp));
                        } else {
                            collector.collect(urlVisit);
                        }
                    }
                });

        processStream.getSideOutput(indexTag).print("index ");
        processStream.getSideOutput(cartTag).print("cart ");
        processStream.print("else ");

        env.execute();

    }

    @Test
    public void testUnion() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Event> stream1 = env.socketTextStream("127.0.0.1", 7777).process(new ProcessFunction<String, Event>() {
            @Override
            public void processElement(String s, ProcessFunction<String, Event>.Context context, Collector<Event> collector) throws Exception {
                collector.collect(new Event(System.currentTimeMillis(), 123L, s));
            }
        });

        SingleOutputStreamOperator<Event> stream2 = env.socketTextStream("127.0.0.1", 7778).process(new ProcessFunction<String, Event>() {
            @Override
            public void processElement(String s, ProcessFunction<String, Event>.Context context, Collector<Event> collector) throws Exception {
                collector.collect(new Event(System.currentTimeMillis(), 234L, s));
            }
        });

        stream1.union(stream2).process(new ProcessFunction<Event, Tuple2<String, Timestamp>>() {
            @Override
            public void processElement(Event event, ProcessFunction<Event, Tuple2<String, Timestamp>>.Context context, Collector<Tuple2<String, Timestamp>> collector) throws Exception {
                collector.collect(Tuple2.of(event.getUrl(), new Timestamp(event.timestamp)));
            }
        }).print();

        env.execute();
    }

    @Test
    public void testConnect() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> stream1 = env.fromElements(1, 2, 3);
        DataStreamSource<Long> stream2 = env.fromElements(1L, 2L, 3L);

        stream1.connect(stream2).map(new CoMapFunction<Integer, Long, String>() {
            @Override
            public String map1(Integer integer) throws Exception {
                return "integer: " + integer;
            }

            @Override
            public String map2(Long aLong) throws Exception {
                return "long: " + aLong;
            }
        }).print();

        env.execute();

    }

    @Test
    public void testBillCheck() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Tuple3<String, String, Long>> stream1 = env.fromElements(
                Tuple3.of("order-1", "app", 1000L),
                Tuple3.of("order-2", "app", 2000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps().withTimestampAssigner((order, time) -> order.f2));

        SingleOutputStreamOperator<Tuple4<String, String, String, Long>> stream2 = env.fromElements(
                Tuple4.of("order-1", "third-party", "success", 3000L),
                Tuple4.of("order-3", "third-party", "success", 4000L)
                ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple4<String, String, String, Long>>forMonotonousTimestamps().withTimestampAssigner((order, time) -> order.f3));

        stream1.connect(stream2)
                .keyBy((e1) -> e1.f0, (e2) -> e2.f0)
                .process(new OrderCheckProcessor())
                .print();
        env.execute();
    }

    @Test
    public void testWindowJoin() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Tuple2<String, Long>> stream1 = env.fromElements(
                Tuple2.of("a", 1000L),
                Tuple2.of("b", 2000L),
                Tuple2.of("c", 3000L),
                Tuple2.of("d", 4000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps().withTimestampAssigner((ele, time) -> ele.f1));

        SingleOutputStreamOperator<Tuple2<String, Long>> stream2 = env.fromElements(
                Tuple2.of("a", 2000L),
                Tuple2.of("b", 3000L),
                Tuple2.of("c", 4000L),
                Tuple2.of("d", 5000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps().withTimestampAssigner((ele, time) -> ele.f1));

        stream1.join(stream2)
                .where(e1 -> e1.f0)
                .equalTo(e2 -> e2.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(6)))
                .apply((left, right) -> left + "=>" + right)
                .print();
        env.execute();
    }

    @Test
    public void testIntervalWindow() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Tuple3<String, String, Long>> stream1 = env.fromElements(
                Tuple3.of("Mary", "order-1", 1000L),
                Tuple3.of("Alice", "order-2", 2000L),
                Tuple3.of("Bob", "order-3", 3000L),
                Tuple3.of("Alice", "order-4", 4000L),
                Tuple3.of("Cary", "order-5", 5000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps().withTimestampAssigner((e, time) -> e.f2));

        SingleOutputStreamOperator<Tuple3<String, String, Long>> stream2 = env.fromElements(
                Tuple3.of("Mary", "/index", 1000L),
                Tuple3.of("Alice", "/index", 2000L),
                Tuple3.of("Bob", "/cart", 3000L),
                Tuple3.of("Alice", "/test", 4000L),
                Tuple3.of("Cary", "/cart", 5000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps().withTimestampAssigner((e, time) -> e.f2));

        stream1.keyBy(data -> data.f0)
                .intervalJoin(stream2.keyBy(e2 -> e2.f0))
                .between(Time.seconds(-5), Time.seconds(10))
                .process(new ProcessJoinFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>() {
                    @Override
                    public void processElement(Tuple3<String, String, Long> left, Tuple3<String, String, Long> right, ProcessJoinFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>.Context context, Collector<String> collector) throws Exception {
                        collector.collect(left + "=>" + right);
                    }
                })
                .print();
        env.execute();
    }

    @Test
    public void testCoGroup() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Tuple2<String, Long>> stream1 = env.fromElements(
                Tuple2.of("a", 1000L),
                Tuple2.of("b", 2000L),
                Tuple2.of("a", 3000L),
                Tuple2.of("b", 4000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps().withTimestampAssigner((ele, time) -> ele.f1));

        SingleOutputStreamOperator<Tuple2<String, Long>> stream2 = env.fromElements(
                Tuple2.of("a", 2000L),
                Tuple2.of("b", 3000L),
                Tuple2.of("a", 4000L),
                Tuple2.of("b", 5000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps().withTimestampAssigner((ele, time) -> ele.f1));

        stream1.coGroup(stream2)
                .where(e1 -> e1.f0)
                .equalTo(e2 -> e2.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply((CoGroupFunction<Tuple2<String, Long>, Tuple2<String, Long>, String>) (e1, e2, collector) -> collector.collect(e1 + "=>" + e2))
                .print();

        env.execute();

    }

    @Test
    public void testPeriodPrint() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KeyedStream<Tuple2<String, Long>, String> stream1 = env.fromElements(
                        Tuple2.of("a", System.currentTimeMillis()),
                        Tuple2.of("b", System.currentTimeMillis()),
                        Tuple2.of("c", System.currentTimeMillis()),
                        Tuple2.of("a", System.currentTimeMillis()),
                        Tuple2.of("b", System.currentTimeMillis())
                ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps().withTimestampAssigner((e, time) -> e.f1))
                .keyBy(e -> e.f0);
        stream1.print("print ");

        stream1.keyBy(e -> e.f0).process(new PeriodPVResult())
                .print();

        env.execute();
    }

    @Test
    public void testCEP() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KeyedStream<Tuple3<String, Boolean, Long>, String> stream = env.fromElements(
                        Tuple3.of("a", false, 1000L),
                        Tuple3.of("a", false, 2000L),
                        Tuple3.of("a", false, 3000L),
                        Tuple3.of("b", true, 4000L),
                        Tuple3.of("b", true, 5000L)
                ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Boolean, Long>>forMonotonousTimestamps().withTimestampAssigner((e, time) -> e.f2))
                .keyBy(e -> e.f0);

        Pattern<Tuple3<String, Boolean, Long>, Tuple3<String, Boolean, Long>> pattern = Pattern
                .<Tuple3<String, Boolean, Long>>begin("first")
                .where(new SimpleCondition<Tuple3<String, Boolean, Long>>() {
                    @Override
                    public boolean filter(Tuple3<String, Boolean, Long> e) throws Exception {
                        return !e.f1;
                    }
                })
                .next("second")
                .where(new SimpleCondition<Tuple3<String, Boolean, Long>>() {
                    @Override
                    public boolean filter(Tuple3<String, Boolean, Long> e) throws Exception {
                        return !e.f1;
                    }
                })
                .next("third")
                .where(new SimpleCondition<Tuple3<String, Boolean, Long>>() {
                    @Override
                    public boolean filter(Tuple3<String, Boolean, Long> e) throws Exception {
                        return !e.f1;
                    }
                });
        PatternStream<Tuple3<String, Boolean, Long>> patternStream = CEP.pattern(stream, pattern);
        patternStream.select((PatternSelectFunction<Tuple3<String, Boolean, Long>, String>) map -> {
                    Tuple3<String, Boolean, Long> first = map.get("first").get(0);
                    Tuple3<String, Boolean, Long> second = map.get("second").get(0);
                    Tuple3<String, Boolean, Long> third = map.get("third").get(0);
                    return first.f0 + " 连续3次登录失败 " + first.f2 + ", " + second.f2 + ", " + third.f2;
                })
                .print("warning:");
        env.execute();
    }
}
