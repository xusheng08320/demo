package com.xusheng.flink.processor;

import com.xusheng.flink.dto.UrlVisitCount;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Author xusheng
 * @Date 2023/2/2 17:32
 * @Desc
 */
public class UrlViewCountResult extends ProcessWindowFunction<Integer, UrlVisitCount, String, TimeWindow> {
    @Override
    public void process(String key, ProcessWindowFunction<Integer, UrlVisitCount, String, TimeWindow>.Context context, Iterable<Integer> iterable, Collector<UrlVisitCount> collector) throws Exception {
        collector.collect(new UrlVisitCount(key, iterable.iterator().next(), context.window().getStart(), context.window().getEnd()));
    }
}
