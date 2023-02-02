package com.xusheng.flink.datasource;

import com.xusheng.flink.URLVisit;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @Author xusheng
 * @Date 2023/2/2 16:26
 * @Desc
 */
public class UrlVisitDataSource implements SourceFunction<URLVisit> {
    @Override
    public void run(SourceContext<URLVisit> sourceContext) throws Exception {
        URLVisit visit1 = new URLVisit("/index", 123L, 1000L);
        URLVisit visit2 = new URLVisit("/index", 123L, 1500L);
        URLVisit visit3 = new URLVisit("/cart", 123L, 1800L);
        URLVisit visit4 = new URLVisit("/index", 123L, 2000L);
        URLVisit visit5 = new URLVisit("/cart", 123L, 2100L);
        sourceContext.collect(visit1);
        sourceContext.collect(visit2);
        sourceContext.collect(visit3);
        sourceContext.collect(visit4);
        sourceContext.collect(visit5);
    }

    @Override
    public void cancel() {

    }
}
