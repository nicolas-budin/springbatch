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
 *       en mémoire.</li>
 * </ul>
 *
 * <p>Le job n'est <b>pas</b> lancé au démarrage (Spring Boot le ferait via
 * {@code JobLauncherCommandLineRunner}, mais {@code spring.batch.job.enabled=false} dans
 * {@code application.properties} le désactive) : c'est Quartz qui le lance toutes les 5 minutes
 * (voir {@link QuartzConfig}).
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
        // SpringApplication.run(...) démarre le contexte Spring, dont Quartz et Tomcat (console H2),
        // puis rend la main. L'application ne s'arrête pas pour autant : les threads de Quartz et de
        // Tomcat (non "daemon") gardent la JVM en vie, et Quartz lance le job toutes les 5 minutes.
        // On l'arrête avec Ctrl+C (ou un kill), ce qui ferme proprement le contexte Spring.
        //
        // Avant l'ajout de l'ordonnanceur, on écrivait :
        //     System.exit(SpringApplication.exit(SpringApplication.run(HelloBatchApplication.class, args)));
        // pour arrêter l'application dès la fin du job et renvoyer un code de sortie (0 = succès)
        // à un ordonnanceur externe (cron, Control-M...). Ici ce n'est plus possible : l'application
        // tourne en continu, il n'y a pas "un" job dont on pourrait renvoyer le résultat.
        SpringApplication.run(HelloBatchApplication.class, args);
    }
}
