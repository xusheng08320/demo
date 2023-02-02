package com.xusheng.flink;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * @Author xusheng
 * @Date 2023/1/31 13:51
 * @Desc
 */
public class Transaction implements Serializable {
    public Long txId;
    public Long accountId;
    public Double amount;
    private Long datetime;

    @Override
    public String toString() {
        return "Transaction{" +
                "txId=" + txId +
                ", accountId=" + accountId +
                ", amount=" + amount +
                ", datetime=" + new Timestamp(datetime) +
                '}';
    }

    public Transaction() {
    }

    public Transaction(Long txId, Long accountId, Double amount, Long datetime) {
        this.txId = txId;
        this.accountId = accountId;
        this.amount = amount;
        this.datetime = datetime;
    }

    public Long getTxId() {
        return txId;
    }

    public void setTxId(Long txId) {
        this.txId = txId;
    }

    public Long getAccountId() {
        return accountId;
    }

    public void setAccountId(Long accountId) {
        this.accountId = accountId;
    }

    public Double getAmount() {
        return amount;
    }

    public void setAmount(Double amount) {
        this.amount = amount;
    }

    public Long getDatetime() {
        return datetime;
    }

    public void setDatetime(Long datetime) {
        this.datetime = datetime;
    }
}
