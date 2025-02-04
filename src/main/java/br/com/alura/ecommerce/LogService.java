package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import javax.rmi.ssl.SslRMIClientSocketFactory;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

public class LogService {
    
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        var consumer = new KafkaConsumer<String, String>(properties());
        consumer.subscribe(Pattern.compile("ECOMMERCE.*"));


        while (true){
            var records = consumer.poll(Duration.ofMillis(100));
            if (!records.isEmpty()) {
                System.out.println("Encontrei " + records.count() + " registros.");
                for (var record : records) {
                    System.out.println("----------------------");
                    System.out.println("LOG: " + record.topic());
                    System.out.println(record.key());
                    System.out.println(record.value());
                    System.out.println(record.partition());
                    System.out.println(record.offset());
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        // ignoring
                        e.printStackTrace();
                    }                   
                }
            }


        }
        
    }

    private static Properties properties() {

        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // Serviços que pertencem a grupos diferentes, estarão escutando todas as mensagens daquele tópico.
        // Serviços que pertencem ao mesmo grupo, estarão "concorrendo" no consumo das mensagens daquele tópico. Eu não tenho certeza de qual serviço irá consumir uma determinada mensagem.
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, LogService.class.getSimpleName());
        return properties;


    }


}
