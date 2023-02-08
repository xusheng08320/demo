package com.xusheng.flink.processor;

import apple.laf.JRSUIState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Types;

/**
 * @Author xusheng
 * @Date 2023/2/6 13:19
 * @Desc
 */
public class PeriodPVResult extends KeyedProcessFunction<String, Tuple2<String, Long>, String> {

    ValueState<Long> countState;
    ValueState<Long> timerState;


    @Override
    public void open(Configuration parameters) throws Exception {
        countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count-state", Long.class));
        timerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-state", Long.class));
    }

    @Override
    public void processElement(Tuple2<String, Long> e, KeyedProcessFunction<String, Tuple2<String, Long>, String>.Context context, Collector<String> collector) throws Exception {
        Long count = countState.value();
        if (count == null) {
            countState.update(1L);
        } else {
            countState.update(count + 1);
        }
        if (timerState.value() == null) {
            context.timerService().registerEventTimeTimer(e.f1 + 10*1000L);
        }
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<String, Tuple2<String, Long>, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
        out.collect(ctx.getCurrentKey() + " pv: " + countState.value());
        timerState.clear();
    }
}
