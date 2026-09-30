package com.example.hellobatch;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Lance {@code helloJob} à intervalles réguliers (toutes les 5 minutes par défaut).
 *
 * <p>Spring Batch ne sait pas planifier un job : il sait seulement l'exécuter quand on le lui
 * demande, via le {@link JobLauncher}. La planification est confiée à un autre outil : ici le
 * scheduler intégré à Spring ({@code @Scheduled}), activé par {@link SchedulingConfig}.
 * (En entreprise, c'est souvent un ordonnanceur externe : cron, Control-M, Kubernetes CronJob...
 * qui lance l'application, le job s'exécute, puis l'application s'arrête.)
 */
@Component
public class HelloJobScheduler {

    // Le JobLauncher est fourni par @EnableBatchProcessing ; le Job est celui déclaré dans HelloJobConfig.
    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private Job helloJob;

    /**
     * Méthode appelée par le scheduler de Spring.
     *
     * <p>L'expression cron a 6 champs (et non 5 comme le cron Unix) :
     * <pre>
     *   seconde  minute  heure  jour-du-mois  mois  jour-de-la-semaine
     *      0      *&#47;5     *         *          *           *
     * </pre>
     * "0 *&#47;5 * * * *" = à la seconde 0 de chaque minute multiple de 5 (12:00, 12:05, 12:10...).
     *
     * <p>La valeur vient de la propriété {@code hello.scheduler.cron} (voir application.properties),
     * ce qui permet de la changer sans recompiler, par exemple toutes les 10 secondes pour tester :
     * {@code --hello.scheduler.cron="*&#47;10 * * * * *"}.
     *
     * <p>Alternative sans cron : {@code @Scheduled(fixedRate = 300000)} (toutes les 300 000 ms,
     * premier lancement dès le démarrage) ou {@code fixedDelay} (5 minutes après la FIN du
     * lancement précédent).
     *
     * <p>Le scheduler par défaut n'a qu'un seul thread : si un lancement dure plus de 5 minutes,
     * le suivant attend qu'il soit terminé. Deux exécutions du job ne se chevauchent donc pas.
     */
    @Scheduled(cron = "${hello.scheduler.cron}")
    public void scheduledLaunch() throws Exception {
        launch();
    }

    /**
     * Lance une nouvelle exécution du job (séparé de la méthode @Scheduled pour pouvoir
     * l'appeler directement dans les tests).
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
