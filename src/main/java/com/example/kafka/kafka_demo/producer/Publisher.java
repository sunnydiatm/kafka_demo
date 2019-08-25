package com.example.kafka.kafka_demo.producer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class Publisher {

    @Autowired
    Schema kafkaAvroSchema ;

    @Autowired
    KafkaEventPublisher publisher;

    private static final String TOPIC_JSON = "Kafka_Example_json";

    public void publishEvent(String name, String dept){

        try{
            Schema metadataSchema = kafkaAvroSchema.getField("metadata").schema();
            GenericRecord metadata = new GenericData.Record(metadataSchema);
            metadata.put("correlationid", "klasfq234qwKAFKATesting");
            metadata.put("subject", "KakfaLearning");
            metadata.put("version", "1.0");

            GenericRecord record = new GenericData.Record(kafkaAvroSchema);
            record.put("metadata", metadata);
            record.put("name", name);
            record.put("dept", dept);
            record.put("empnumber", "1234567");

            publisher.sendToKafka(new ProducerRecord<>(TOPIC_JSON, record));

            System.out.println("message send successfully=======" );

        } catch (Exception e) {
            System.out.println("Error occured while sending messages to Kafka ==> " +e);

        }
    }


}
