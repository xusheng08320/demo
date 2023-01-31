package com.xusheng.flink;

/**
 * @Author xusheng
 * @Date 2023/1/31 13:53
 * @Desc
 */
public class Alert {
    public Long id;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Alert(Long id) {
        this.id = id;
    }

    public Alert() {
    }

    @Override
    public String toString() {
        return "Alert{" +
                "id=" + id +
                '}';
    }
}
