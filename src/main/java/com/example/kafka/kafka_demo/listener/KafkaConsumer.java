package com.example.kafka.kafka_demo.listener;

import com.example.kafka.kafka_demo.model.Header;
import com.example.kafka.kafka_demo.model.User;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.List;

@Service
public class KafkaConsumer {

    @KafkaListener(topics = "Kafka_Example", groupId = "group_id")
    public void consume(String message) {
        System.out.println("Consumed message: " + message);
    }

   /* @KafkaListener(topics = "Kafka_Example_json", groupId = "group_json", containerFactory = "userKafkaListenerFactory")
    public void consumeJson(@Payload  User user) {

        System.out.println("Consumed JSON Message:   " + user );
    }


    @KafkaListener(topics = "Kafka_Example_json", groupId = "group_json", containerFactory = "userKafkaListenerFactory")
    public void consumeJson(ConsumerRecord<String, User> consumerRecord) {
    try {
        User user = consumerRecord.value();
        System.out.println("Consumed JSON Message:   " + user);
        System.out.println("consumerRecord.topic():   " + consumerRecord.topic());
        System.out.println("consumerRecord.partition():   " + consumerRecord.partition());
        System.out.println("consumerRecord.headers():   " + consumerRecord.headers());
        System.out.println("consumerRecord.key():   " + consumerRecord.key());
        System.out.println("consumerRecord.timestamp():   " + consumerRecord.timestamp());
        System.out.println("consumerRecord.timestampType():   " + consumerRecord.timestampType());
        Headers header = consumerRecord.headers();
        Header[] headerArray = header.toArray();
        System.out.println("header length:   " + headerArray.length);
    }catch (Exception e){
        System.out.println("Exception while reading message from Kafka " + e );
    }
  }*/

    @KafkaListener(topics = "Kafka_Example_json", containerFactory = "genericKafkaListenerFactory")
    public void consumeJson(ConsumerRecord<String, GenericRecord> consumerRecord) {
        try {
            GenericRecord generic = consumerRecord.value();
            Object obj = generic.get("metadata");

            //Schema schema = consumerRecord.value().getSchema();

            //Schema metadata1 = schema.getField("metadata").schema();
            //GenericRecord record = new GenericData.Record(metadata1);
            //Object obj = record.get("correlationid");


            ObjectMapper mapper = new ObjectMapper();

            Header headerMetaData = mapper.readValue(obj.toString(), Header.class);

            System.out.println("Received payload :   " + consumerRecord.value());
            System.out.println("Received genericMetadata :   " + generic.get("metadata").toString());
            System.out.println("Received name  :   " + generic.get("name").toString());
            System.out.println("Received correlationid  :   " + headerMetaData.getCorrelationid());
        }catch (Exception e){
            System.out.println("Exception while reading message from Kafka " + e );
        }
    }

}
