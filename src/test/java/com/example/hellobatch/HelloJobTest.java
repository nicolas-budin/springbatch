package com.example.hellobatch;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * Test d'intégration du job : on démarre tout le contexte Spring, on lance le job
 * et on vérifie son résultat grâce aux métadonnées enregistrées par Spring Batch.
 */
// JUnit 4 (celui de Spring Boot 1.5) : @RunWith(SpringRunner.class) branche Spring sur JUnit.
// (Avec JUnit 5, c'est @SpringBootTest seul qui s'en charge.)
@RunWith(SpringRunner.class)
// @SpringBootTest : démarre le contexte Spring complet, comme l'application.
// C'est le test qui doit décider quand lancer le job, pas le démarrage ni l'horloge :
//  - spring.batch.job.enabled=false : pas de lancement automatique au démarrage
//    (déjà dans application.properties, mais on le rend explicite ici) ;
//  - hello.scheduler.enabled=false : pas de scheduler (voir SchedulingConfig).
@SpringBootTest(properties = {"spring.batch.job.enabled=false", "hello.scheduler.enabled=false"})
public class HelloJobTest {

    /**
     * Configuration ajoutée au contexte, uniquement pour les tests.
     *
     * <p>En Spring Batch 3, il faut déclarer soi-même le bean {@code JobLauncherTestUtils}
     * (l'annotation {@code @SpringBatchTest}, qui le fait automatiquement, n'apparaît
     * qu'en Spring Batch 4.1). Ses setters sont annotés {@code @Autowired} : Spring lui
     * injecte l'unique bean {@code Job} (helloJob), le {@code JobLauncher} et le {@code JobRepository}.
     */
    @TestConfiguration
    static class BatchTestConfig {
        @Bean
        public JobLauncherTestUtils jobLauncherTestUtils() {
            return new JobLauncherTestUtils();
        }
    }

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Test
    public void jobCompletesAndProcessesAllNames() throws Exception {
        // launchJob() lance le job avec des paramètres uniques (un nombre aléatoire),
        // ce qui crée à chaque fois une nouvelle JobInstance : le test peut donc être
        // rejoué sans erreur "job déjà terminé".
        // L'appel est synchrone : on récupère le JobExecution une fois le job terminé.
        JobExecution execution = jobLauncherTestUtils.launchJob();

        // Le job entier doit être terminé avec succès.
        assertThat(execution.getStatus()).isEqualTo(BatchStatus.COMPLETED);

        // Un JobExecution contient un StepExecution par step exécuté,
        // avec ses compteurs (lus, écrits, filtrés, commits, rollbacks...).
        StepExecution greetStep = execution.getStepExecutions().stream()
                .filter(s -> s.getStepName().equals("greetStep"))
                .findFirst().orElseThrow(IllegalStateException::new);
        assertThat(greetStep.getReadCount()).isEqualTo(5);   // 5 noms lus
        assertThat(greetStep.getWriteCount()).isEqualTo(5);  // 5 salutations écrites
        assertThat(greetStep.getCommitCount()).isEqualTo(3); // chunks de 2 : 2 + 2 + 1 = 3 commits
    }
}
