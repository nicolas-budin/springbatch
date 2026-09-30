package com.example.hellobatch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Point d'entrée de l'application.
 *
 * <p>Il n'y a presque rien à écrire ici : l'auto-configuration de Spring Boot
 * (activée par {@code spring-boot-starter-batch}) s'occupe de tout :
 * <ul>
 *   <li>elle crée l'infrastructure Spring Batch : {@code JobRepository},
 *       {@code JobLauncher}, {@code PlatformTransactionManager}...</li>
 *   <li>elle crée les tables de métadonnées Spring Batch ({@code BATCH_*})
 *       dans la base H2 en mémoire ;</li>
 *   <li>au démarrage, elle trouve le bean {@code Job} déclaré dans
 *       {@link HelloJobConfig} et le lance automatiquement
 *       (via {@code JobLauncherApplicationRunner}).</li>
 * </ul>
 *
 * <p>Attention : avec Spring Boot 3, il ne faut PAS ajouter
 * {@code @EnableBatchProcessing}. Cette annotation désactive
 * l'auto-configuration Batch de Spring Boot (et donc le lancement
 * automatique du job et la création des tables).
 */
@SpringBootApplication
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
