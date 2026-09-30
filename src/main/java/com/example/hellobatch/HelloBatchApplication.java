package com.example.hellobatch;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Point d'entrée de l'application.
 *
 * <p>Il n'y a presque rien à écrire ici. Le travail est partagé entre deux mécanismes :
 * <ul>
 *   <li>{@code @EnableBatchProcessing} (Spring Batch) crée l'infrastructure Spring Batch :
 *       {@code JobRepository}, {@code JobLauncher}, {@code PlatformTransactionManager},
 *       ainsi que les fabriques {@code JobBuilderFactory} et {@code StepBuilderFactory}
 *       utilisées dans {@link HelloJobConfig} ;</li>
 *   <li>l'auto-configuration de Spring Boot (activée par {@code spring-boot-starter-batch})
 *       crée les tables de métadonnées Spring Batch ({@code BATCH_*}) dans la base H2
 *       en mémoire, puis, au démarrage, trouve le bean {@code Job} déclaré dans
 *       {@link HelloJobConfig} et le lance automatiquement
 *       (via {@code JobLauncherCommandLineRunner}).</li>
 * </ul>
 *
 * <p>Attention : avec Spring Boot 1.x / Spring Batch 3, {@code @EnableBatchProcessing} est
 * <b>obligatoire</b>. C'est l'inverse avec Spring Boot 3 / Spring Batch 5, où cette même
 * annotation désactive l'auto-configuration : un piège classique quand on lit du code
 * écrit pour une autre version.
 */
@SpringBootApplication
@EnableBatchProcessing
public class HelloBatchApplication {

    public static void main(String[] args) {
        // SpringApplication.run(...) démarre le contexte Spring : c'est là que le job s'exécute.
        // Une fois le job terminé, il n'y a plus rien à faire (pas de serveur web),
        // donc on ferme proprement le contexte avec SpringApplication.exit(...).
        // Celui-ci renvoie un code de sortie : 0 si tout s'est bien passé, non nul si le job
        // a échoué. On le transmet au système avec System.exit(...), ce qui est pratique
        // quand le batch est lancé par un ordonnanceur (cron, Control-M, Kubernetes CronJob...).
        System.exit(SpringApplication.exit(SpringApplication.run(HelloBatchApplication.class, args)));
    }
}
