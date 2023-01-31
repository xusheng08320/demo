package com.xusheng.flink;

import java.io.Serializable;

/**
 * @Author xusheng
 * @Date 2023/1/31 13:51
 * @Desc
 */
public class Transaction implements Serializable {
    public Long txId;
    public Long accountId;
    public Double amount;

    public Transaction(Long txId, Long accountId, Double amount) {
        this.txId = txId;
        this.accountId = accountId;
        this.amount = amount;
    }

    @Override
    public String toString() {
        return "Transaction{" +
                "txId=" + txId +
                ", accountId=" + accountId +
                ", amount=" + amount +
                '}';
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
}
