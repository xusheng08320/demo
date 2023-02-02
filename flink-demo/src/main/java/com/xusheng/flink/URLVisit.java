package com.xusheng.flink;

import java.sql.Timestamp;

/**
 * @Author xusheng
 * @Date 2023/2/2 16:26
 * @Desc
 */
public class URLVisit {
    public String url;
    public Long bosId;
    public Long timestamp;

    @Override
    public String toString() {
        return "URLVisit{" +
                "url='" + url + '\'' +
                ", bosId=" + bosId +
                ", timestamp=" + new Timestamp(timestamp) +
                '}';
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Long getBosId() {
        return bosId;
    }

    public void setBosId(Long bosId) {
        this.bosId = bosId;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public URLVisit() {
    }

    public URLVisit(String url, Long bosId, Long timestamp) {
        this.url = url;
        this.bosId = bosId;
        this.timestamp = timestamp;
    }
}
