package br.com.alura.ecommerce;


import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {


    public static void main(String[] args) throws ExecutionException, InterruptedException {


        for (int i=0; i<10; i++){
            var producer = new KafkaProducer<String, String>(properties());
            var key = UUID.randomUUID().toString();
            var value = "447,465,1852";
            //Passar como parâmetro nome do tópico, chave e mensagem
            var record = new ProducerRecord<>("ECOMMERCE_NEW_ORDER", key, value);
            //record = mensagem/registro que será salvo no Kafka. Configurações de tempo de armazenamento ou outros, são configurações do server.properties
            //send é assíncrono. Com o Get esperamos o future retornar
            Callback callback = (data, ex) -> {
                if(ex != null) {
                    ex.printStackTrace();
                    return;
                }
                System.out.println("Sucesso enviado: " + data.topic() + "::: Partition " + data.partition() + " / offset " + data.offset() + " /  timestamp " + data.timestamp());
            };
            var email = "Enviando email serviço KAFKA";
            var emailRecord = new ProducerRecord<>("ECOMMERCE_SEND_EMAIL", key, email);
            producer.send(record, callback).get();
            producer.send(emailRecord, callback).get();
        }
    }

    private static Properties properties() {
        var properties = new Properties();
        //Local onde estão rodando os meus 'Kafkas'
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        //Serializador - Para a chave, transfoma String em Bytes
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //Serializador - Para a mensagem/valores transforma a String em Bytes
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;

    }


}
