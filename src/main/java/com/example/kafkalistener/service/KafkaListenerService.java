//package com.example.kafkalistener.service;
//
//import com.example.kafkalistener.entity.MessageRecord;
//import com.example.kafkalistener.repository.MessageRecordRepository;
//import com.fasterxml.jackson.databind.JsonNode;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.springframework.kafka.annotation.KafkaListener;
//import org.springframework.stereotype.Service;
//
//@Service
//public class KafkaListenerService {
//    private final MessageRecordRepository messageRecordRepository;
//    private final ObjectMapper objectMapper = new ObjectMapper();  // Можно сразу инициализировать
//
//    public KafkaListenerService(MessageRecordRepository messageRecordRepository) {
//        this.messageRecordRepository = messageRecordRepository;
//    }
//
//    @KafkaListener(topics = "Franz_Kafka", groupId = "group-id")
//    public void listen(ConsumerRecord<String, String> record) {
//        try {
//            JsonNode jsonNode = objectMapper.readTree(record.value());
//
//            // Извлекаем msg_id и timestamp как числа
//            int msgId = jsonNode.get("msg_id").asInt();
//            long timestamp = jsonNode.get("timestamp").asLong();
//
//            MessageRecord messageRecord = new MessageRecord();
//            messageRecord.setMsgId(msgId);  // msgId теперь Integer
//            messageRecord.setTimeRq(String.valueOf(timestamp));  // timeRq теперь Long
//            messageRecordRepository.save(messageRecord);
//
//            System.out.println("Message saved: msgId=" + msgId + ", timeRq=" + timestamp);
//        } catch (Exception e) {
//            System.err.println("Error processing message: " + record.value());
//            e.printStackTrace();
//        }
//    }
//}

package com.example.kafkalistener.service;

import com.example.kafkalistener.entity.MessageRecord;
import com.example.kafkalistener.repository.MessageRecordRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaListenerService {
    private final MessageRecordRepository messageRecordRepository;
    private final ObjectMapper objectMapper = new ObjectMapper();  // Инициализация ObjectMapper

    public KafkaListenerService(MessageRecordRepository messageRecordRepository) {
        this.messageRecordRepository = messageRecordRepository;
    }

    @KafkaListener(topics = "Franz_Kafka", groupId = "group-id")
    public void listen(ConsumerRecord<String, String> record) {
        try {
            JsonNode jsonNode = objectMapper.readTree(record.value());

            // Извлекаем msg_id и timestamp
            String msgId = jsonNode.get("msg_id").asText();  // Теперь msgId - это String
            String timestamp = jsonNode.get("timestamp").asText();

            // Сохранение записи в базу данных
            MessageRecord messageRecord = new MessageRecord();
            messageRecord.setMsgId(msgId);  // Присваиваем msgId
            messageRecord.setTimeRq(timestamp);  // Присваиваем timestamp в строковом формате

            // Сохраняем объект в базе данных
            messageRecordRepository.save(messageRecord);

            System.out.println("Message saved: msgId=" + msgId + ", timeRq=" + timestamp);
        } catch (Exception e) {
            System.err.println("Error processing message: " + record.value());
            e.printStackTrace();
        }
    }
}


