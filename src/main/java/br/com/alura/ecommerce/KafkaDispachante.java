package br.com.alura.ecommerce;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaDispachante implements Closeable{
    
    private final KafkaProducer<String, String> producer;

    KafkaDispachante() {
        this.producer = new KafkaProducer<>(properties());
    }

    void send(String topic, String key, String value) throws InterruptedException, ExecutionException{

        //Passar como parâmetro nome do tópico, chave e mensagem
            var record = new ProducerRecord<>(topic, key, value);
            //record = mensagem/registro que será salvo no Kafka. Configurações de tempo de armazenamento ou outros, são configurações do server.properties
            //send é assíncrono. Com o Get esperamos o future retornar
            Callback callback = (data, ex) -> {
                if(ex != null) {
                    ex.printStackTrace();
                    return;
                }
                System.out.println("Sucesso enviado: " + data.topic() + "::: Partition " + data.partition() + " / offset " + data.offset() + " /  timestamp " + data.timestamp());
            };
            producer.send(record, callback).get();

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

    @Override
    public void close(){
        producer.close();
    }



}
