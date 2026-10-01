package com.example.hellobatch;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.quartz.QuartzJobBean;

/**
 * Le "job Quartz" : ce que Quartz exécute à chaque déclenchement du trigger (voir {@link QuartzConfig}).
 *
 * <p><b>Attention au vocabulaire : il y a deux sortes de "jobs".</b>
 * <ul>
 *   <li>le <b>job Quartz</b> (cette classe) dit <i>quand</i> faire quelque chose : c'est une
 *       simple tâche planifiée, qui ne sait rien de Spring Batch ;</li>
 *   <li>le <b>job Spring Batch</b> ({@code helloJob}, dans {@link HelloJobConfig}) dit <i>quoi</i>
 *       faire : steps, chunks, reader/processor/writer, métadonnées en base.</li>
 * </ul>
 * Ici, le job Quartz se contente de lancer le job Spring Batch.
 *
 * <p><b>Ce n'est pas un bean Spring.</b> Quartz crée lui-même une <i>nouvelle instance</i> de
 * cette classe à chaque déclenchement. Pour que le champ {@code @Autowired} ci-dessous soit
 * rempli, {@link QuartzConfig} configure Quartz avec une "JobFactory" qui demande à Spring
 * d'injecter les dépendances dans chaque instance créée. Sans elle, {@code scheduler} serait
 * {@code null} (NullPointerException au premier déclenchement).
 *
 * <p><b>{@code @DisallowConcurrentExecution}</b> : Quartz exécute ses jobs dans un pool de
 * 10 threads. Si un lancement durait plus de 5 minutes, le suivant démarrerait en parallèle
 * sur un autre thread. Cette annotation l'interdit : le déclenchement suivant attend la fin du
 * précédent. (Avec {@code @Scheduled}, c'était implicite : un seul thread.)
 */
@DisallowConcurrentExecution
public class HelloQuartzJob extends QuartzJobBean {

    @Autowired
    private HelloJobScheduler scheduler;

    /**
     * Appelée par Quartz à chaque déclenchement du trigger (sur un thread de son pool).
     *
     * @param context informations sur le déclenchement (trigger, heure prévue, prochaine heure...)
     */
    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        try {
            scheduler.launch();
        } catch (Exception e) {
            // Quartz n'accepte que des JobExecutionException : on "emballe" l'erreur. Quartz la
            // journalise, et le trigger continue de se déclencher aux échéances suivantes.
            throw new JobExecutionException("Échec du lancement de helloJob", e);
        }
    }
}
