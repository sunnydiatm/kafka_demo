package com.example.kafka.kafka_demo;


import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.springframework.kafka.test.hamcrest.KafkaMatchers.hasPartition;

public class KafkaClusterTest {
    private static final String TEMPLATE_TOPIC = "templateTopic";

    String jsonSchema = "{\n"+
            "   \"type\":\"record\",\n"+
            "   \"name\":\"KafkaEvent\",\n"+
            "   \"namespace\":\"com.abc.model.avro\",\n"+
            "   \"fields\":[\n"+
            "      {\n"+
            "         \"name\":\"metadata\",\n"+
            "         \"type\":{\n"+
            "            \"name\":\"metadata\",\n"+
            "            \"type\":\"record\",\n"+
            "            \"fields\":[\n"+
            "               {\n"+
            "                  \"name\":\"correlationid\",\n"+
            "                  \"type\":\"string\",\n"+
            "                  \"doc\":\"this is corrleation id for transaction\"\n"+
            "               },\n"+
            "               {\n"+
            "                  \"name\":\"subject\",\n"+
            "                  \"type\":\"string\",\n"+
            "                  \"doc\":\"this is subject for transaction\"\n"+
            "               },\n"+
            "               {\n"+
            "                  \"name\":\"version\",\n"+
            "                  \"type\":\"string\",\n"+
            "                  \"doc\":\"this is version for transaction\"\n"+
            "               }\n"+
            "            ]\n"+
            "         }\n"+
            "      },\n"+
            "      {\n"+
            "         \"name\":\"name\",\n"+
            "         \"type\":\"string\"\n"+
            "      },\n"+
            "      {\n"+
            "         \"name\":\"dept\",\n"+
            "         \"type\":\"string\"\n"+
            "      },\n"+
            "      {\n"+
            "         \"name\":\"empnumber\",\n"+
            "         \"type\":\"string\"\n"+
            "      }\n"+
            "   ]\n"+
            "}";

    @ClassRule
    public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, TEMPLATE_TOPIC);

    @Test
    public void testTemplate() throws Exception {


        Schema.Parser parser = new Schema.Parser();
        Schema kafkaAvroSchema = parser.parse(jsonSchema);
        Schema metadataSchema = kafkaAvroSchema.getField("metadata").schema();
        GenericRecord metadata = new GenericData.Record(metadataSchema);
        metadata.put("correlationid", "klasfq234qwKAFKATesting");
        metadata.put("subject", "KakfaLearning");
        metadata.put("version", "1.0");

        GenericRecord record = new GenericData.Record(kafkaAvroSchema);
        record.put("metadata", metadata);
        record.put("name", "name");
        record.put("dept", "dept");
        record.put("empnumber", "1234567");

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("testT", "false", embeddedKafka);
        DefaultKafkaConsumerFactory<String, GenericRecord> cf = new DefaultKafkaConsumerFactory<String, GenericRecord>(consumerProps);
        ContainerProperties containerProperties = new ContainerProperties(TEMPLATE_TOPIC);
        KafkaMessageListenerContainer<String, GenericRecord> container = new KafkaMessageListenerContainer<>(cf, containerProperties);
        final BlockingQueue<ConsumerRecord<String, GenericRecord>> records = new LinkedBlockingQueue<>();
        container.setupMessageListener(new MessageListener<String, GenericRecord>() {

            @Override
            public void onMessage(ConsumerRecord<String, GenericRecord> record) {
                System.out.println(record);
                records.add(record);
            }

        });
        container.setBeanName("templateTests");
        container.start();
        ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
        Map<String, Object> senderProps = KafkaTestUtils.senderProps(embeddedKafka.getBrokersAsString());
        senderProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        senderProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        ProducerFactory<String, GenericRecord> pf = new DefaultKafkaProducerFactory<String, GenericRecord>(senderProps);
        KafkaTemplate<String, GenericRecord> template = new KafkaTemplate<>(pf);
        template.setDefaultTopic(TEMPLATE_TOPIC);
        template.sendDefault(record);
       //assertThat(records.poll(10, TimeUnit.SECONDS), hasValue("foo"));
        //template.sendDefault(0, "1", record);
        ConsumerRecord<String, GenericRecord> received = records.poll(10, TimeUnit.SECONDS);
       // assertThat(received, hasKey(2));
        assertThat(received, hasPartition(0));
        //assertThat(received, hasValue("bar"));
       // template.send(TEMPLATE_TOPIC, 0, 2, "baz");
        //received = records.poll(10, TimeUnit.SECONDS);
       // assertThat(received, hasKey(2));
        //assertThat(received, hasPartition(0));
        //assertThat(received, hasValue("baz"));
    }

}
