package com.xusheng.flink.datasource;

import com.xusheng.flink.Event;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @Author xusheng
 * @Date 2023/2/1 11:13
 * @Desc
 */
public class EventDataSource implements SourceFunction<Event> {

    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {
        Event event1 = new Event(1000L, 123L, "/index");
        Event event2 = new Event(1100L, 123L, "/index");
        Event event3 = new Event(2000L, 123L, "/shop");
        Event event4 = new Event(3000L, 123L, "/cart");
        Event event5 = new Event(5000L, 123L, "/index");
        Event event6 = new Event(6000L, 123L, "/index");
        Event event7 = new Event(7000L, 123L, "/index");

        sourceContext.collect(event1);
        sourceContext.collect(event2);
        sourceContext.collect(event3);
        sourceContext.collect(event4);
        sourceContext.collect(event5);
        sourceContext.collect(event6);
        sourceContext.collect(event7);
    }

    @Override
    public void cancel() {

    }
}
