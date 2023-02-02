package com.xusheng.flink.dto;

/**
 * @Author xusheng
 * @Date 2023/2/2 17:26
 * @Desc
 */
public class UrlVisitCount {
    public String url;
    public Long bosId;
    public Integer count;
    public Long windowStart;
    public Long windowEnd;

    @Override
    public String toString() {
        return "UrlVisitCount{" +
                "url='" + url + '\'' +
                ", bosId=" + bosId +
                ", count=" + count +
                ", windowStart=" + windowStart +
                ", windowEnd=" + windowEnd +
                '}';
    }

    public Long getWindowStart() {
        return windowStart;
    }

    public void setWindowStart(Long windowStart) {
        this.windowStart = windowStart;
    }

    public Long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Long windowEnd) {
        this.windowEnd = windowEnd;
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

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public UrlVisitCount() {
    }

    public UrlVisitCount(String url, Long bosId, Integer count) {
        this.url = url;
        this.bosId = bosId;
        this.count = count;
    }

    public UrlVisitCount(String url, Integer count, Long windowStart, Long windowEnd) {
        this.url = url;
        this.count = count;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
    }
}
