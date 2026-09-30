package com.example.hellobatch;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * Test d'intégration du job : on démarre tout le contexte Spring, on lance le job
 * et on vérifie son résultat grâce aux métadonnées enregistrées par Spring Batch.
 */
// @SpringBootTest : démarre le contexte Spring complet, comme l'application.
// spring.batch.job.enabled=false : désactive le lancement automatique du job au démarrage.
// Sinon le job tournerait une première fois au démarrage du contexte, puis une seconde
// fois dans le test. C'est le test qui doit décider quand lancer le job.
@SpringBootTest(properties = "spring.batch.job.enabled=false")
// @SpringBatchTest : ajoute des outils de test Spring Batch au contexte,
// notamment JobLauncherTestUtils (utilisé ci-dessous) et JobRepositoryTestUtils.
@SpringBatchTest
class HelloJobTest {

    // JobLauncherTestUtils détecte automatiquement l'unique bean Job du contexte (helloJob).
    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Test
    void jobCompletesAndProcessesAllNames() throws Exception {
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
                .findFirst().orElseThrow();
        assertThat(greetStep.getReadCount()).isEqualTo(5);   // 5 noms lus
        assertThat(greetStep.getWriteCount()).isEqualTo(5);  // 5 salutations écrites
        assertThat(greetStep.getCommitCount()).isEqualTo(3); // chunks de 2 : 2 + 2 + 1 = 3 commits
    }
}
