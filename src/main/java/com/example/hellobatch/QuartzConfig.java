package com.example.hellobatch;

import org.quartz.JobDetail;
import org.quartz.Trigger;
import org.quartz.spi.TriggerFiredBundle;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.quartz.CronTriggerFactoryBean;
import org.springframework.scheduling.quartz.JobDetailFactoryBean;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;
import org.springframework.scheduling.quartz.SpringBeanJobFactory;

/**
 * Configuration de Quartz, l'ordonnanceur qui lance helloJob toutes les 5 minutes.
 *
 * <p>Quartz s'organise en trois briques :
 * <pre>
 *   JobDetail  : QUOI exécuter       -> la classe HelloQuartzJob
 *   Trigger    : QUAND l'exécuter    -> une expression cron (hello.scheduler.cron)
 *   Scheduler  : le moteur           -> surveille les triggers et exécute les jobs dans son pool de threads
 * </pre>
 * Un même JobDetail peut avoir plusieurs triggers (par exemple toutes les 5 minutes + tous les
 * soirs à 22h), et le Scheduler peut gérer beaucoup de couples JobDetail/Trigger.
 *
 * <p><b>Planifications en mémoire.</b> Aucune DataSource n'est donnée au SchedulerFactoryBean :
 * Quartz garde donc ses planifications en mémoire ({@code RAMJobStore}), perdues à l'arrêt, et
 * recréées au démarrage à partir de cette configuration. Avec une DataSource, il les stockerait
 * dans des tables {@code QRTZ_*} : c'est ce qui permet le mode cluster (une seule instance de
 * l'application exécute chaque déclenchement) et le rattrapage des échéances manquées pendant un
 * arrêt. Inutile ici : une seule instance, et une base H2 elle-même en mémoire.
 *
 * <p>{@code @ConditionalOnProperty} permet de couper Quartz avec {@code hello.scheduler.enabled=false}
 * (actif si la propriété est absente). Les tests s'en servent : ce sont eux qui décident quand
 * lancer le job, pas l'horloge.
 *
 * <p>Spring Boot 1.5 n'a pas d'auto-configuration Quartz (elle n'arrive qu'en Spring Boot 2.0) :
 * tout est déclaré ici, à la main, avec les classes d'intégration de {@code spring-context-support}.
 */
@Configuration
@ConditionalOnProperty(name = "hello.scheduler.enabled", matchIfMissing = true)
public class QuartzConfig {

    /**
     * QUOI : le job Quartz à exécuter.
     *
     * <p>{@code durability = true} : le JobDetail reste enregistré même s'il n'a plus aucun
     * trigger (sinon Quartz le supprimerait).
     */
    @Bean
    public JobDetailFactoryBean helloJobDetail() {
        JobDetailFactoryBean factory = new JobDetailFactoryBean();
        factory.setJobClass(HelloQuartzJob.class);
        factory.setName("helloQuartzJob");
        factory.setDurability(true);
        return factory;
    }

    /**
     * QUAND : un trigger cron, dont l'expression vient de {@code hello.scheduler.cron}.
     *
     * <p><b>Le cron de Quartz n'est pas tout à fait celui de Spring.</b> Mêmes 6 champs
     * (seconde minute heure jour-du-mois mois jour-de-la-semaine), mais l'un des deux champs
     * "jour" doit valoir {@code ?} ("peu importe") : Quartz refuse qu'on précise à la fois un
     * jour du mois et un jour de la semaine. Ainsi "0 *&#47;5 * * * *" (valable pour
     * {@code @Scheduled}) est refusé au démarrage ; il faut écrire "0 *&#47;5 * * * ?".
     *
     * @param jobDetail le JobDetail déclaré ci-dessus (Spring passe l'objet créé par la fabrique)
     */
    @Bean
    public CronTriggerFactoryBean helloTrigger(JobDetail jobDetail,
                                               @Value("${hello.scheduler.cron}") String cron) {
        CronTriggerFactoryBean factory = new CronTriggerFactoryBean();
        factory.setJobDetail(jobDetail);
        factory.setName("helloTrigger");
        factory.setCronExpression(cron);
        return factory;
    }

    /**
     * Le moteur Quartz : démarré avec le contexte Spring, arrêté proprement avec lui.
     *
     * @param trigger            le trigger déclaré ci-dessus
     * @param applicationContext le contexte Spring, pour injecter les dépendances dans les jobs Quartz
     */
    @Bean
    public SchedulerFactoryBean schedulerFactoryBean(JobDetail jobDetail, Trigger trigger,
                                                     ApplicationContext applicationContext) {
        SchedulerFactoryBean factory = new SchedulerFactoryBean();
        factory.setJobDetails(jobDetail);
        factory.setTriggers(trigger);
        factory.setJobFactory(new AutowiringSpringBeanJobFactory(applicationContext));
        // À l'arrêt de l'application (Ctrl+C), attendre la fin d'un job en cours plutôt que de
        // le couper au milieu (le job Spring Batch finirait sinon en statut incohérent).
        factory.setWaitForJobsToCompleteOnShutdown(true);
        return factory;
    }

    /**
     * Fabrique de jobs Quartz qui injecte les beans Spring ({@code @Autowired}) dans chaque
     * instance de job créée par Quartz.
     *
     * <p>Quartz instancie lui-même la classe du job à chaque déclenchement, en dehors de Spring :
     * les champs {@code @Autowired} ne seraient donc jamais remplis. Cette fabrique crée l'instance
     * normalement ({@code super.createJobInstance}), puis demande à Spring de l'"autowirer".
     * (En Spring 4.3, {@link SpringBeanJobFactory} ne le fait pas d'elle-même ; les versions
     * récentes de Spring, et l'auto-configuration Quartz de Spring Boot 2, le font directement.)
     */
    static class AutowiringSpringBeanJobFactory extends SpringBeanJobFactory {

        private final ApplicationContext applicationContext;

        AutowiringSpringBeanJobFactory(ApplicationContext applicationContext) {
            this.applicationContext = applicationContext;
        }

        @Override
        protected Object createJobInstance(TriggerFiredBundle bundle) throws Exception {
            Object job = super.createJobInstance(bundle);
            applicationContext.getAutowireCapableBeanFactory().autowireBean(job);
            return job;
        }
    }
}
