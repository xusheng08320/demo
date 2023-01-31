package com.xusheng.flink.processor;

import com.xusheng.flink.Alert;
import com.xusheng.flink.Transaction;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;

/**
 * @Author xusheng
 * @Date 2023/1/31 14:09
 * @Desc
 */
public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

    private static final double SMALL_AMOUNT = 1.00;
    private static final double LARGE_AMOUNT = 500.00;
    private static final long ONE_MINUTE = 60 * 1000;

    private ValueState<Boolean> flagState;
    private ValueState<Long> timerState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor flagDesc = new ValueStateDescriptor("flag", Types.BOOLEAN);
        flagState = getRuntimeContext().getState(flagDesc);

        ValueStateDescriptor timerDesc = new ValueStateDescriptor("timer-state", Types.LONG);
        timerState = getRuntimeContext().getState(timerDesc);
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<Long, Transaction, Alert>.OnTimerContext ctx, Collector<Alert> out) throws Exception {
        timerState.clear();
        flagState.clear();
    }

    @Override
    public void processElement(Transaction transaction, KeyedProcessFunction<Long, Transaction, Alert>.Context context, Collector<Alert> collector) throws Exception {
        Boolean lastTransactionWasSmall = flagState.value();
        if (BooleanUtils.isTrue(lastTransactionWasSmall)) {
            if (transaction.amount > LARGE_AMOUNT) {
                Alert alert = new Alert(transaction.getTxId());
                collector.collect(alert);
            }
            cleanUp(context);
        }
        if (transaction.amount < SMALL_AMOUNT) {
            flagState.update(true);

            long timer = context.timerService().currentProcessingTime() + ONE_MINUTE;
            context.timerService().registerProcessingTimeTimer(timer);
            timerState.update(timer);
        }
    }

    public void cleanUp(Context context) throws IOException {
        Long timer = timerState.value();
        context.timerService().deleteEventTimeTimer(timer);

        timerState.clear();
        flagState.clear();
    }

}
