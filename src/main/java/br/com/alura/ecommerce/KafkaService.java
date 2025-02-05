package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;


class KafkaService implements Closeable {

    private final KafkaConsumer<String, String> consumer;
    private final ConsumerFunction parse;
    
    KafkaService(String grupoID,String topic, ConsumerFunction parse){
        this.parse = parse;
        this.consumer = new KafkaConsumer<String, String>(properties(grupoID));
        consumer.subscribe(Collections.singletonList(topic));
    }

    void run(){
        while (true){
            var records = consumer.poll(Duration.ofMillis(100));
            if (!records.isEmpty()) {
                System.out.println("Encontrei " + records.count() + " registros.");
                for (var record : records) {
                   parse.consume(record);
                }
            }


        }
    }


    private static Properties properties(String grupoID) {

        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // Serviços que pertencem a grupos diferentes, estarão escutando todas as mensagens daquele tópico.
        // Serviços que pertencem ao mesmo grupo, estarão "concorrendo" no consumo das mensagens daquele tópico. Eu não tenho certeza de qual serviço irá consumir uma determinada mensagem.
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, grupoID);
         properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        //Propriedade de máximo de records que quero consumir para poder realizar um commit/informar ao Kafka o quanto já foi consumido.
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        
        
        return properties;


    }

    @Override
    public void close() {
        consumer.close();
    }

}