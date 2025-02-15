package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import java.util.concurrent.ExecutionException;

public class EmailService {
    
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        //service vai ser inscrito no topico passado no parâmetro e possui o registro/record da aplicação especificada.
        var emailService = new EmailService();
        try(var service = new KafkaService(EmailService.class.getSimpleName(),
                "ECOMMERCE_SEND_EMAIL", 
                emailService::parse)){
            service.run();
        }
        
    }

    private void parse(ConsumerRecord<String, String> record){

        System.out.println("----------------------");
        System.out.println("Enviar email.");
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
        System.out.println("Email processado");

    }

}
