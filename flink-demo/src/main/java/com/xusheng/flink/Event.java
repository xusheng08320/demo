package com.xusheng.flink;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * @Author xusheng
 * @Date 2023/2/1 11:12
 * @Desc
 */
public class Event implements Serializable {

    public Long timestamp;
    public Long bosId;
    public String url;

    @Override
    public String toString() {
        return "Event{" +
                "time=" + new Timestamp(timestamp) +
                ",timestamp=" + timestamp +
                ", bosId=" + bosId +
                ", url='" + url + '\'' +
                '}';
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Long getBosId() {
        return bosId;
    }

    public void setBosId(Long bosId) {
        this.bosId = bosId;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Event(Long timestamp, Long bosId, String url) {
        this.timestamp = timestamp;
        this.bosId = bosId;
        this.url = url;
    }
}
