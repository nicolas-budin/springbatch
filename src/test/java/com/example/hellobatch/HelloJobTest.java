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

// spring.batch.job.enabled=false : pas de lancement auto au démarrage, c'est le test qui lance le job.
@SpringBootTest(properties = "spring.batch.job.enabled=false")
@SpringBatchTest
class HelloJobTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Test
    void jobCompletesAndProcessesAllNames() throws Exception {
        JobExecution execution = jobLauncherTestUtils.launchJob();

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.COMPLETED);

        StepExecution greetStep = execution.getStepExecutions().stream()
                .filter(s -> s.getStepName().equals("greetStep"))
                .findFirst().orElseThrow();
        assertThat(greetStep.getReadCount()).isEqualTo(5);
        assertThat(greetStep.getWriteCount()).isEqualTo(5);
        assertThat(greetStep.getCommitCount()).isEqualTo(3); // chunks : 2 + 2 + 1
    }
}
