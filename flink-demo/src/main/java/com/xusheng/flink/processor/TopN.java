package com.xusheng.flink.processor;

import com.xusheng.flink.dto.UrlVisitCount;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @Author xusheng
 * @Date 2023/2/2 17:24
 * @Desc
 */
public class TopN extends KeyedProcessFunction<String, UrlVisitCount, String> {

    private Integer n;

    public TopN(Integer n) {
        this.n = n;
    }

    private ListState<UrlVisitCount> urlVisitCountListState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ListStateDescriptor descriptor = new ListStateDescriptor<UrlVisitCount>("url-view-count-list", UrlVisitCount.class);
        urlVisitCountListState = getRuntimeContext().getListState(descriptor);
    }

    @Override
    public void processElement(UrlVisitCount urlVisitCount, KeyedProcessFunction<String, UrlVisitCount, String>.Context context, Collector<String> collector) throws Exception {
        urlVisitCountListState.add(urlVisitCount);
        List<UrlVisitCount> list = new ArrayList<>();
        urlVisitCountListState.get().forEach(list::add);
        context.timerService().registerProcessingTimeTimer(urlVisitCount.windowEnd + 1);
        list.sort(Comparator.comparingInt(e -> e.count));
        StringBuilder result = new StringBuilder();
        result.append("========================================\n");
        for (int i = 0; i < n && i < list.size(); i++) {
            UrlVisitCount count = list.get(i);
            String info = "No." + (i + 1) + " "
                    + "url：" + count.url + " "
                    + "浏览量：" + count.count + " "
                    + "窗口开始时间：" + new Timestamp(count.windowStart) + " "
                    + "窗口结束时间：" + new Timestamp(count.windowEnd) + "\n";
            result.append(info);
        }
        collector.collect(result.toString());
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<String, UrlVisitCount, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
        urlVisitCountListState.clear();
    }
}
