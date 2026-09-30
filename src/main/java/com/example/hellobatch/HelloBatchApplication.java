package com.example.hellobatch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Point d'entrée. Spring Boot détecte le bean {@code Job} et le lance
 * automatiquement au démarrage, puis l'application s'arrête.
 */
@SpringBootApplication
public class HelloBatchApplication {

    public static void main(String[] args) {
        System.exit(SpringApplication.exit(SpringApplication.run(HelloBatchApplication.class, args)));
    }
}
