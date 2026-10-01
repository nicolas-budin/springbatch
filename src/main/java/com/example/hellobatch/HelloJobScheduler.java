package com.example.hellobatch;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Lance une nouvelle exécution de {@code helloJob}.
 *
 * <p>Spring Batch ne sait pas planifier un job : il sait seulement l'exécuter quand on le lui
 * demande, via le {@link JobLauncher}. La planification est confiée à un autre outil : ici
 * Quartz, qui appelle {@link #launch()} toutes les 5 minutes par l'intermédiaire de
 * {@link HelloQuartzJob} (voir {@link QuartzConfig}).
 * (En entreprise, c'est aussi souvent un ordonnanceur externe : cron, Control-M, Kubernetes
 * CronJob... qui lance l'application, le job s'exécute, puis l'application s'arrête.)
 *
 * <p>Cette classe ne dépend pas de Quartz : les tests l'appellent directement, sans ordonnanceur.
 */
@Component
public class HelloJobScheduler {

    // Le JobLauncher est fourni par @EnableBatchProcessing ; le Job est celui déclaré dans HelloJobConfig.
    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private Job helloJob;

    /**
     * Lance une nouvelle exécution du job.
     *
     * <p><b>Pourquoi un paramètre "launchTime" ?</b> Une JobInstance est identifiée par le nom du
     * job + ses paramètres. Si on relançait le job avec les mêmes paramètres (par exemple aucun),
     * Spring Batch considérerait que c'est la même JobInstance, déjà terminée, et refuserait avec
     * {@code JobInstanceAlreadyCompleteException} dès le 2e lancement. L'heure courante rend
     * chaque lancement unique, et indique en plus dans BATCH_JOB_EXECUTION_PARAMS quand il a eu lieu.
     */
    public JobExecution launch() throws Exception {
        JobParameters parameters = new JobParametersBuilder()
                .addLong("launchTime", System.currentTimeMillis())
                .toJobParameters();
        // Appel synchrone : on ne sort de run(...) qu'une fois le job terminé.
        return jobLauncher.run(helloJob, parameters);
    }
}
