package com.example.kafka.kafka_demo.producer;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Repository;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Repository
public class KafkaEventPublisher {

    @Autowired
    private KafkaTemplate<String, GenericRecord> kafkaTemplate;

    public void sendToKafka(ProducerRecord<String, GenericRecord>  producerRecord) {
        try {
            ListenableFuture<SendResult<String, GenericRecord>> future = kafkaTemplate.send(producerRecord);
            future.addCallback(new ListenableFutureCallback<SendResult<String, GenericRecord>>() {
                @Override
                public void onSuccess(SendResult<String, GenericRecord> result) {
                    System.out.println("Successfully sent message:   " + result.toString());
                }

                @Override
                public void onFailure(Throwable ex) {
                    System.out.println("Sending message failed   " + ex);
                }

            });
        }catch (Exception e){
            System.out.println("Sending to kafka failed ==>  " + e);
        }
    }
}
