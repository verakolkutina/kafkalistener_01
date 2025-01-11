package com.example.kafkalistener.entity;


import jakarta.persistence.Entity;
import jakarta.persistence.Id;

@Entity
public class MessageRecord {

    @Id
    private String msgId;
    private String timeRq;  // timestamp, но будет сохранен как String ( timestamp в формате строки)

    // Геттеры и сеттеры
    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public String getTimeRq() {
        return timeRq;
    }

    public void setTimeRq(String timeRq) {
        this.timeRq = timeRq;
    }
}