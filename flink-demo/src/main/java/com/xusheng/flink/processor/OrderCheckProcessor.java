package com.xusheng.flink.processor;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @Author xusheng
 * @Date 2023/2/3 15:30
 * @Desc
 */
public class OrderCheckProcessor extends CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String> {

    private ValueState<Tuple3<String, String, Long>> appEventState;
    private ValueState<Tuple4<String, String, String, Long>> thirdPartyEventState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor appEventStateDesc = new ValueStateDescriptor("app-event-state", Types.TUPLE(Types.STRING, Types.STRING, Types.LONG));
        appEventState = getRuntimeContext().getState(appEventStateDesc);

        ValueStateDescriptor thridPartyEventStateDesc = new ValueStateDescriptor("third-party-event-state", Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.LONG));
        thirdPartyEventState = getRuntimeContext().getState(thridPartyEventStateDesc);
    }

    @Override
    public void processElement1(Tuple3<String, String, Long> e1, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.Context context, Collector<String> collector) throws Exception {
        if (thirdPartyEventState.value() != null) {
            collector.collect("对账结果：" + thirdPartyEventState.value().f2 + " 账单：" + e1.f0);
            thirdPartyEventState.clear();
            return;
        }
        appEventState.update(e1);
        context.timerService().registerEventTimeTimer(e1.f2 + 5000L);
    }

    @Override
    public void processElement2(Tuple4<String, String, String, Long> e2, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.Context context, Collector<String> collector) throws Exception {
        if (appEventState.value() != null) {
            collector.collect("对账结果：" + e2.f2 + " 账单：" + e2.f0);
            appEventState.clear();
            return;
        }
        thirdPartyEventState.update(e2);
        context.timerService().registerEventTimeTimer(e2.f3 + 5000L);
    }

    @Override
    public void onTimer(long timestamp, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
        if (appEventState.value() != null) {
            out.collect("第三方对账结果没有: " + appEventState.value().f0);
        }
        else if (thirdPartyEventState.value() != null) {
            out.collect("订单结果没有: " + thirdPartyEventState.value().f0);
        } else {
            out.collect("2个结果都未空: " ) ;
        }
        appEventState.clear();
        thirdPartyEventState.clear();
    }
}
