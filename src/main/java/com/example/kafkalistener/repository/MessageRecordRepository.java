package com.example.kafkalistener.repository;

import com.example.kafkalistener.entity.MessageRecord;
import org.springframework.data.jpa.repository.JpaRepository;

public interface MessageRecordRepository  extends JpaRepository<MessageRecord, String> {

}
