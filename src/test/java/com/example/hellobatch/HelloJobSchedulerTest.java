package com.example.hellobatch;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * Vérifie que le job peut être lancé plusieurs fois de suite dans la même application,
 * comme le fait Quartz toutes les 5 minutes.
 *
 * <p>On n'attend pas 5 minutes : Quartz est désactivé et on appelle directement
 * {@link HelloJobScheduler#launch()}, la méthode que le job Quartz ({@link HelloQuartzJob}) appelle.
 */
@RunWith(SpringRunner.class)
// Comme dans HelloJobTest : pas de lancement au démarrage, pas de Quartz.
@SpringBootTest(properties = {"spring.batch.job.enabled=false", "hello.scheduler.enabled=false"})
public class HelloJobSchedulerTest {

    @Autowired
    private HelloJobScheduler scheduler;

    @Test
    public void jobCanBeLaunchedSeveralTimesInARow() throws Exception {
        JobExecution first = scheduler.launch();
        // Petite pause : le paramètre "launchTime" est en millisecondes, deux lancements
        // dans la même milliseconde auraient les mêmes paramètres (même JobInstance).
        Thread.sleep(5);
        JobExecution second = scheduler.launch();

        // Sans le paramètre "launchTime", le 2e lancement échouerait avec
        // JobInstanceAlreadyCompleteException (même job, mêmes paramètres = même JobInstance).
        assertThat(first.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertThat(second.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertThat(second.getJobInstance().getId()).isNotEqualTo(first.getJobInstance().getId());

        // Sans @StepScope sur le reader, le 2e lancement ne lirait rien : le reader (singleton)
        // serait resté positionné à la fin de la table PERSON après le 1er lancement.
        assertThat(readCountOfGreetStep(second)).isEqualTo(5);
    }

    private int readCountOfGreetStep(JobExecution execution) {
        for (StepExecution step : execution.getStepExecutions()) {
            if (step.getStepName().equals("greetStep")) {
                return step.getReadCount();
            }
        }
        throw new IllegalStateException("greetStep non exécuté");
    }
}
