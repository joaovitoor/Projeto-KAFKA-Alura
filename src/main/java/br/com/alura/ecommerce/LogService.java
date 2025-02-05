package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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

        var logService = new LogService();
        var service = new KafkaService(LogService.class.getSimpleName(), "ECOMMERCE.*", logService::parse);
        service.run();
        
    }

    private void parse(ConsumerRecord<String,String> record){
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
