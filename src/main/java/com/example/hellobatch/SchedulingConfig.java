package com.example.hellobatch;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Active le scheduler de Spring : sans {@code @EnableScheduling}, les méthodes
 * {@code @Scheduled} (comme celle de {@link HelloJobScheduler}) ne sont jamais appelées.
 *
 * <p>{@code @ConditionalOnProperty} permet de couper le scheduler avec
 * {@code hello.scheduler.enabled=false} (actif si la propriété est absente :
 * {@code matchIfMissing = true}). Les tests s'en servent : ce sont eux qui décident
 * quand lancer le job, pas l'horloge.
 *
 * <p>C'est pour pouvoir le désactiver ainsi que {@code @EnableScheduling} est dans sa propre
 * classe, et pas sur {@link HelloBatchApplication}.
 */
@Configuration
@EnableScheduling
@ConditionalOnProperty(name = "hello.scheduler.enabled", matchIfMissing = true)
public class SchedulingConfig {
}
