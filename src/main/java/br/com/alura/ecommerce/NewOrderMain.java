package br.com.alura.ecommerce;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {


    public static void main(String[] args) throws ExecutionException, InterruptedException {

        try(var dispachante = new KafkaDispachante()){

            for (int i=0; i<10; i++){

                var key = UUID.randomUUID().toString();
                var value = "447,465,1852";
                dispachante.send("ECOMMERCE_NEW_ORDER", key, value);
                
                var email = "Enviando email serviço KAFKA";
                dispachante.send("ECOMMERCE_SEND_EMAIL", email, email);
                
            }
        }
    }



}
