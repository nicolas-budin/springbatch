# Le scheduler Quartz

Ce document explique comment `helloJob` est lancé automatiquement toutes les 5 minutes, avec **Quartz**. Pour le job Spring Batch lui-même (steps, chunks, reader...), voir le [README](README.md).

Les logs cités ici sont de vrais logs de l'application, lancée avec `logging.level.org.quartz=DEBUG` et un cron de 10 secondes. Ils sont légèrement simplifiés : date, PID et noms de classes abrégés.

---

## Sommaire

1. [En bref](#1-en-bref)
2. [Les pièces du puzzle](#2-les-pièces-du-puzzle)
3. [Au démarrage](#3-au-démarrage)
4. [À chaque échéance](#4-à-chaque-échéance)
5. [L'expression cron](#5-lexpression-cron)
6. [Threads et chevauchement](#6-threads-et-chevauchement)
7. [Échéances manquées et stockage des planifications](#7-échéances-manquées-et-stockage-des-planifications)
8. [À l'arrêt](#8-à-larrêt)
9. [Changer la fréquence, désactiver, tester](#9-changer-la-fréquence-désactiver-tester)
10. [Observer ce que fait Quartz](#10-observer-ce-que-fait-quartz)
11. [Pièges](#11-pièges)
12. [Pour aller plus loin](#12-pour-aller-plus-loin)

---

## 1. En bref

Spring Batch **ne sait pas planifier** un job : il sait seulement l'exécuter quand on le lui demande. Quartz s'occupe du « quand » :

```
                  toutes les 5 minutes
  Quartz ─────────────────────────────▶ HelloQuartzJob ──▶ HelloJobScheduler.launch() ──▶ jobLauncher.run(helloJob)
  (QuartzConfig)                          job Quartz          paramètre launchTime            job Spring Batch
                                          = QUAND             unique                          = QUOI
```

Il y a **deux sortes de « jobs »** dans ce projet, à ne pas confondre :

| | Job **Quartz** | Job **Spring Batch** |
|---|---|---|
| Classe / bean | `HelloQuartzJob` | `helloJob` (`HelloJobConfig`) |
| Rôle | Dit **quand** faire quelque chose | Dit **quoi** faire : steps, chunks, reader, processor, writer |
| Connaît l'autre ? | Oui, il lance le job Spring Batch | Non, il ignore qui le lance |
| Traces | Logs Quartz | Logs + tables `BATCH_*` |

---

## 2. Les pièces du puzzle

| Fichier | Rôle |
|---|---|
| [`QuartzConfig.java`](src/main/java/com/example/hellobatch/QuartzConfig.java) | Configure Quartz : `JobDetail`, `Trigger`, `Scheduler`, et la *JobFactory* |
| [`HelloQuartzJob.java`](src/main/java/com/example/hellobatch/HelloQuartzJob.java) | Le job Quartz, exécuté à chaque échéance |
| [`HelloJobScheduler.java`](src/main/java/com/example/hellobatch/HelloJobScheduler.java) | `launch()` : lance une exécution du job Spring Batch. Ne dépend pas de Quartz |
| [`application.properties`](src/main/resources/application.properties) | `hello.scheduler.cron=0 */5 * * * ?` (la fréquence) et `spring.batch.job.enabled=false` (pas de lancement au démarrage) |
| [`pom.xml`](pom.xml) | Dépendances `quartz` (2.3.2) et `spring-context-support` |

Quartz s'organise en **trois briques**, toutes déclarées dans `QuartzConfig` :

| Brique | Question | Ici | Déclarée avec |
|---|---|---|---|
| **JobDetail** | **Quoi** exécuter ? | la classe `HelloQuartzJob`, nommée `helloQuartzJob` | `JobDetailFactoryBean` |
| **Trigger** | **Quand** ? | un cron lu dans `hello.scheduler.cron`, nommé `helloTrigger` | `CronTriggerFactoryBean` |
| **Scheduler** | Qui surveille et exécute ? | le moteur Quartz, avec son pool de threads | `SchedulerFactoryBean` |

Un même JobDetail peut avoir **plusieurs triggers** (par exemple toutes les 5 minutes en journée, plus une fois à 22h), et un Scheduler peut gérer de nombreux couples JobDetail / Trigger.

### Pourquoi tout est configuré à la main

Spring Boot 1.5 **ne connaît pas Quartz** :
- pas de version gérée, d'où `<version>2.3.2</version>` dans le `pom.xml` ;
- pas d'auto-configuration : le `spring-boot-starter-quartz` n'existe qu'à partir de Spring Boot 2.0.

Les classes `*FactoryBean`, `QuartzJobBean` et `SpringBeanJobFactory` viennent de `spring-context-support`, le module d'intégration de Quartz dans Spring.

### La *JobFactory* : injecter des beans dans un job Quartz

Quartz crée **lui-même** une nouvelle instance de `HelloQuartzJob` à chaque échéance, en dehors de Spring. Les champs `@Autowired` ne seraient donc jamais remplis. `QuartzConfig` donne à Quartz une *JobFactory* qui corrige ça :

```java
static class AutowiringSpringBeanJobFactory extends SpringBeanJobFactory {
    @Override
    protected Object createJobInstance(TriggerFiredBundle bundle) throws Exception {
        Object job = super.createJobInstance(bundle);                         // Quartz crée l'instance...
        applicationContext.getAutowireCapableBeanFactory().autowireBean(job); // ...Spring remplit les @Autowired
        return job;
    }
}
```

On la voit au démarrage : `JobFactory set to: com.example.hellobatch.QuartzConfig$AutowiringSpringBeanJobFactory@...`.

Sans elle, le champ `scheduler` de `HelloQuartzJob` serait `null` : `NullPointerException` à la première échéance. En Spring 4.3, la `SpringBeanJobFactory` de base ne fait pas cette injection ; les versions récentes de Spring, et l'auto-configuration de Spring Boot 2, la font directement.

---

## 3. Au démarrage

Quand le contexte Spring se crée, le `SchedulerFactoryBean` construit le moteur Quartz. Il ne le **démarre** qu'une fois tout le contexte prêt : aucun job ne peut donc partir avant que les beans (`JobLauncher`, `helloJob`...) existent.

```
INFO  [main] QuartzScheduler     : Quartz Scheduler v.2.3.2 created.
INFO  [main] RAMJobStore         : RAMJobStore initialized.
INFO  [main] QuartzScheduler     : Scheduler meta-data: Quartz Scheduler (v2.3.2) 'schedulerFactoryBean' with instanceId 'NON_CLUSTERED'
  Scheduler class: 'org.quartz.core.QuartzScheduler' - running locally.
  Using thread pool 'org.quartz.simpl.SimpleThreadPool' - with 10 threads.
  Using job-store 'org.quartz.simpl.RAMJobStore' - which does not support persistence. and is not clustered.
INFO  [main] QuartzScheduler     : JobFactory set to: com.example.hellobatch.QuartzConfig$AutowiringSpringBeanJobFactory@2c177f9e
INFO  [main] SchedulerFactoryBean: Starting Quartz Scheduler now
INFO  [main] QuartzScheduler     : Scheduler schedulerFactoryBean_$_NON_CLUSTERED started.
```

Ce résumé indique :
- le **nom** du scheduler (`schedulerFactoryBean`, le nom du bean Spring) ;
- le **pool de 10 threads** qui exécuteront les jobs ;
- le **stockage** des planifications, `RAMJobStore`, c'est-à-dire en mémoire (voir [section 7](#7-échéances-manquées-et-stockage-des-planifications)) ;
- `NON_CLUSTERED` : une seule instance, pas de coordination avec d'autres serveurs.

Le job **ne part pas** au démarrage : il attend la première échéance du cron. Si on démarre à 12:03, le premier lancement a lieu à 12:05.

---

## 4. À chaque échéance

Quartz utilise deux sortes de threads :
- **un thread `SchedulerThread`**, qui surveille les triggers : il calcule la prochaine échéance, attend, puis la confie à un thread du pool ;
- **les 10 threads `schedulerFactoryBean_Worker-N`**, qui exécutent réellement les jobs.

```
SchedulerThread                           Worker-1                                   Spring Batch
     │ "batch acquisition of 1 triggers"
     │ (attend l'échéance : 12:05:00)
     │──── confie l'exécution ───────────────▶│
     │                                        │ crée un HelloQuartzJob (JobFactory + @Autowired)
     │                                        │ executeInternal()
     │                                        │   └─ HelloJobScheduler.launch() ──────────▶ helloJob : helloStep, greetStep
     │                                        │                                            (synchrone)
     │                                        │◀────────────────────────────────────────── COMPLETED
     │◀── terminé ────────────────────────────│
     │ "batch acquisition of 1 triggers"
     │ (attend l'échéance suivante : 12:10:00)
```

Dans les logs, avec un cron de 10 secondes :

```
DEBUG [SchedulerThread] QuartzSchedulerThread: batch acquisition of 1 triggers
DEBUG [Worker-1]        JobRunShell          : Calling execute on job DEFAULT.helloQuartzJob
INFO  [Worker-1]        SimpleJobLauncher    : Job: [SimpleJob: [name=helloJob]] launched with the following parameters: [{launchTime=1790833600024}]
INFO  [Worker-1]        SimpleJobLauncher    : Job: [SimpleJob: [name=helloJob]] completed ... and the following status: [COMPLETED]
DEBUG [SchedulerThread] QuartzSchedulerThread: batch acquisition of 1 triggers
DEBUG [Worker-2]        JobRunShell          : Calling execute on job DEFAULT.helloQuartzJob
INFO  [Worker-2]        SimpleJobLauncher    : Job: [SimpleJob: [name=helloJob]] launched with the following parameters: [{launchTime=1790833610004}]
...
```

À noter :
- `DEFAULT.helloQuartzJob` : chaque job Quartz a un **groupe** (`DEFAULT` si on n'en donne pas) et un **nom**, celui donné dans `QuartzConfig`.
- **Une échéance n'utilise pas toujours le même thread** : `Worker-1`, puis `Worker-2`... Le pool distribue le travail.
- **`launchTime` change à chaque fois.** C'est indispensable : une JobInstance Spring Batch est identifiée par le nom du job et ses paramètres. Avec des paramètres identiques, la 2e échéance serait refusée (`JobInstanceAlreadyCompleteException`).
- Si le job Spring Batch échoue, `HelloQuartzJob` « emballe » l'erreur dans une `JobExecutionException`. Quartz la journalise, et **le trigger continue** de se déclencher aux échéances suivantes.

---

## 5. L'expression cron

### Les champs

Un cron Quartz a **6 champs**, plus un 7e optionnel :

```
 ┌───────────── seconde        (0-59)
 │ ┌─────────── minute         (0-59)
 │ │  ┌──────── heure          (0-23)
 │ │  │ ┌────── jour du mois   (1-31)
 │ │  │ │ ┌──── mois           (1-12 ou JAN-DEC)
 │ │  │ │ │ ┌── jour semaine   (1-7 ou SUN-SAT, 1 = dimanche)
 │ │  │ │ │ │  (année, optionnelle)
 0 */5 * * * ?
```

`0 */5 * * * ?` : à la seconde 0, de chaque minute multiple de 5, de toutes les heures, quel que soit le jour → 12:00:00, 12:05:00, 12:10:00...

### Les caractères spéciaux

| Caractère | Sens | Exemple |
|---|---|---|
| `*` | toutes les valeurs | `*` en heure = toutes les heures |
| `?` | **peu importe**, seulement pour jour du mois / jour de la semaine | voir ci-dessous |
| `,` | liste | `0,30` en minute = à 0 et 30 |
| `-` | intervalle | `9-17` en heure = de 9h à 17h |
| `/` | pas | `*/5` ou `0/5` en minute = toutes les 5 minutes |
| `L` | dernier | `L` en jour du mois = dernier jour du mois |
| `W` | jour ouvré le plus proche | `15W` = le jour ouvré le plus proche du 15 |
| `#` | n-ième jour de la semaine du mois | `6#3` = le 3e vendredi du mois |

### La règle du `?`

Quartz exige que **l'un des deux champs « jour » vaille `?`** : il refuse qu'on précise à la fois un jour du mois et un jour de la semaine.

| Expression | Valide pour Quartz ? |
|---|---|
| `0 */5 * * * ?` | ✅ |
| `0 0 22 ? * MON-FRI` | ✅ (jour du mois = `?`) |
| `0 */5 * * * *` | ❌ l'application ne démarre pas |

Le message d'erreur, au démarrage :

```
java.text.ParseException: Support for specifying both a day-of-week AND a day-of-month parameter is not implemented.
```

### Exemples

| Besoin | Expression |
|---|---|
| Toutes les 5 minutes (la valeur du projet) | `0 */5 * * * ?` |
| Toutes les 10 secondes (pour tester) | `*/10 * * * * ?` |
| Tous les jours à 2h du matin | `0 0 2 * * ?` |
| Du lundi au vendredi à 22h | `0 0 22 ? * MON-FRI` |
| Tous les quarts d'heure, de 9h à 17h, en semaine | `0 0/15 9-17 ? * MON-FRI` |
| Le 1er de chaque mois à 6h | `0 0 6 1 * ?` |
| Le dernier jour du mois à 23h | `0 0 23 L * ?` |

### Différences avec les autres crons

| | Cron Unix (crontab) | Spring `@Scheduled` | Quartz |
|---|---|---|---|
| Nombre de champs | 5 (sans les secondes) | 6 | 6 (+ année optionnelle) |
| `?` | non | accepté, équivaut à `*` | **obligatoire** dans un des champs « jour » |
| Dimanche | 0 ou 7 | 0 ou 7 | **1** (et samedi = 7) |
| `L`, `W`, `#` | non | non (Spring 4.3) | oui |

Pour le jour de la semaine, utilise les **noms** (`MON`, `FRI`...) plutôt que les chiffres : la numérotation diffère d'un outil à l'autre.

---

## 6. Threads et chevauchement

Quartz exécute les jobs dans un **pool de 10 threads**. Sans précaution, si un lancement durait plus de 5 minutes, l'échéance suivante démarrerait **en parallèle** sur un autre thread, et deux exécutions de `helloJob` tourneraient en même temps.

`HelloQuartzJob` est donc annoté **`@DisallowConcurrentExecution`** : Quartz n'exécute jamais deux instances du même JobDetail en même temps. L'échéance suivante attend la fin de la précédente.

```
sans @DisallowConcurrentExecution          avec @DisallowConcurrentExecution
12:00 ████████████████ (7 min)             12:00 ████████████████ (7 min)
12:05      ████████ ← en parallèle !        12:05         ⏳ attend...  ████████ (démarre à 12:07)
```

Le scheduler `@Scheduled` de Spring, utilisé dans une version précédente du projet, n'avait qu'un seul thread : le chevauchement était impossible sans rien faire. Avec Quartz, il faut y penser.

---

## 7. Échéances manquées et stockage des planifications

### Échéance manquée (*misfire*)

Une échéance est **manquée** quand Quartz n'a pas pu la déclencher à l'heure. C'est le cas quand tous les threads étaient occupés, ou quand l'exécution précédente n'était pas finie (`@DisallowConcurrentExecution`). Quartz ne considère une échéance comme manquée qu'au-delà d'un **seuil** (propriété Quartz `org.quartz.jobStore.misfireThreshold`) : ici **5 secondes**, la valeur par défaut de `RAMJobStore`. Un retard plus court déclenche simplement l'échéance en retard. (On lit souvent « 60 secondes » : c'est la valeur du fichier `quartz.properties` livré avec Quartz, que le `SchedulerFactoryBean` de Spring ne charge pas. Le log de démarrage le dit : *initialized from an externally provided properties instance*.)

Pour un trigger cron, la politique par défaut est : **déclencher une seule fois tout de suite**, puis reprendre le rythme normal. Si 3 échéances ont été manquées, le job ne tourne pas 3 fois d'affilée. On peut choisir une autre politique sur le `CronTriggerFactoryBean` (`setMisfireInstruction(...)`), par exemple ignorer les échéances manquées.

### `RAMJobStore` : planifications en mémoire

Le *job store*, c'est l'endroit où Quartz range ses JobDetails, ses triggers et l'heure de leur prochaine échéance. Ici, aucune `DataSource` n'est donnée au `SchedulerFactoryBean`, donc Quartz utilise le **`RAMJobStore`**, en mémoire :
- à l'arrêt de l'application, tout est perdu ;
- au démarrage, tout est recréé à partir de `QuartzConfig` ;
- une échéance qui tombait pendant que l'application était arrêtée est simplement **perdue** : Quartz n'en a gardé aucune trace.

### Le stockage en base (JDBC) et le mode cluster

En donnant une `DataSource` au `SchedulerFactoryBean` (`setDataSource(...)`), Quartz range ses planifications dans des tables **`QRTZ_*`** (`QRTZ_JOB_DETAILS`, `QRTZ_TRIGGERS`, `QRTZ_CRON_TRIGGERS`, `QRTZ_FIRED_TRIGGERS`, `QRTZ_LOCKS`...). Les scripts de création sont fournis dans le jar de Quartz. Ce stockage permet :
- de **survivre aux redémarrages**, et de rattraper au redémarrage une échéance manquée pendant l'arrêt (selon la politique de *misfire*) ;
- le **mode cluster** : plusieurs instances de l'application partagent les mêmes tables et se coordonnent grâce à des verrous en base (`QRTZ_LOCKS`). **Une seule** instance exécute chaque échéance.

Sans ce mode, si l'application tournait sur 3 serveurs, chacun lancerait `helloJob` toutes les 5 minutes : **3 exécutions** au lieu d'une.

Dans ce projet, ce stockage n'aurait pas d'intérêt : une seule instance, et une base H2 elle-même en mémoire, qui disparaît à l'arrêt. C'est aussi pour ça que `c3p0` et `HikariCP-java7`, deux pools de connexions que Quartz n'utilise que pour ce stockage, sont exclus dans le `pom.xml`.

---

## 8. À l'arrêt

À l'arrêt de l'application (Ctrl+C), Spring ferme le contexte, et le `SchedulerFactoryBean` arrête Quartz :

```
INFO  [Thread-3] QuartzScheduler     : Scheduler schedulerFactoryBean_$_NON_CLUSTERED paused.
INFO  [Thread-3] SchedulerFactoryBean: Shutting down Quartz Scheduler
INFO  [Thread-3] QuartzScheduler     : Scheduler schedulerFactoryBean_$_NON_CLUSTERED shutting down.
DEBUG [Thread-3] SimpleThreadPool    : Shutting down threadpool...
DEBUG [Worker-1] SimpleThreadPool    : WorkerThread is shut down.
...  (les 10 workers)
DEBUG [Thread-3] SimpleThreadPool    : No executing jobs remaining, all threads stopped.
INFO  [Thread-3] QuartzScheduler     : Scheduler schedulerFactoryBean_$_NON_CLUSTERED shutdown complete.
```

1. Quartz se met en **pause** : plus aucune nouvelle échéance n'est déclenchée.
2. Grâce à `setWaitForJobsToCompleteOnShutdown(true)` dans `QuartzConfig`, il **attend la fin d'un job en cours** au lieu de le couper au milieu. Sinon, l'exécution Spring Batch resterait en statut `STARTED` dans `BATCH_JOB_EXECUTION`, sans jamais se terminer.
3. Les 10 threads du pool s'arrêtent, puis la JVM peut s'arrêter.

---

## 9. Changer la fréquence, désactiver, tester

| Besoin | Comment |
|---|---|
| Changer la fréquence | Modifier `hello.scheduler.cron` dans `application.properties` |
| Changer la fréquence sans recompiler | Argument au lancement : `java -jar target/hello-batch-0.0.1-SNAPSHOT.jar "--hello.scheduler.cron=*/10 * * * * ?"` (les guillemets sont nécessaires à cause des espaces et des `*`) |
| Désactiver Quartz | `hello.scheduler.enabled=false` : `QuartzConfig` porte `@ConditionalOnProperty`, il n'est alors pas chargé du tout |
| Lancer le job une fois au démarrage, en plus | Remettre `spring.batch.job.enabled=true` : Spring Boot le lance au démarrage, puis Quartz prend le relais |

### Dans les tests

Les tests désactivent Quartz (`hello.scheduler.enabled=false`) : **c'est le test qui décide quand lancer le job**, pas l'horloge. Un test qui attendrait 5 minutes serait lent et fragile.

`HelloJobSchedulerTest` appelle donc directement `HelloJobScheduler.launch()`, la méthode que le job Quartz appelle, deux fois de suite. Il vérifie ainsi ce que Quartz fait à chaque échéance : deux exécutions `COMPLETED`, deux JobInstances différentes grâce à `launchTime`, et 5 noms lus à chaque fois grâce à `@StepScope` sur le reader.

C'est pour ça que `launch()` est dans une classe à part, sans dépendance à Quartz : on peut la tester sans ordonnanceur.

---

## 10. Observer ce que fait Quartz

Par défaut, Quartz n'écrit que le résumé du démarrage et de l'arrêt. Pour voir chaque échéance, ajoute dans `application.properties` (ou en argument `--logging.level.org.quartz=DEBUG`) :

```properties
logging.level.org.quartz=DEBUG
```

| Ligne de log | Signification |
|---|---|
| `Scheduler ... started.` | Quartz tourne |
| `batch acquisition of 1 triggers` | le `SchedulerThread` a pris le prochain trigger et attend son échéance |
| `batch acquisition of 0 triggers` | aucun trigger à déclencher dans l'immédiat (répété régulièrement, c'est normal) |
| `Calling execute on job DEFAULT.helloQuartzJob` | l'échéance est arrivée, un worker exécute le job Quartz |
| `Job: [SimpleJob: [name=helloJob]] launched ...` | le job Spring Batch démarre (log de Spring Batch) |
| `Job DEFAULT.helloQuartzJob threw a JobExecutionException:` | le lancement du job Spring Batch a échoué (erreur « emballée » par `HelloQuartzJob`). Regarder la cause (`Caused by`) |
| `Scheduler ... shutdown complete.` | Quartz est arrêté |

Pour savoir si une exécution a bien eu lieu, la référence reste la table `BATCH_JOB_EXECUTION`, à consulter dans la console H2 (http://localhost:8080/h2-console, voir le [README](README.md#consulter-la-base-avec-la-console-h2)) : une ligne par échéance, avec son statut.

---

## 11. Pièges

| Piège | Symptôme | Solution |
|---|---|---|
| Cron au format Spring ou Unix | `ParseException: Support for specifying both a day-of-week AND a day-of-month parameter is not implemented`, l'application ne démarre pas | Mettre `?` dans un des champs « jour » : `0 */5 * * * ?` |
| Pas de *JobFactory* qui injecte les beans | `NullPointerException` à la première échéance : les `@Autowired` du job Quartz sont `null` | `setJobFactory(new AutowiringSpringBeanJobFactory(...))` (voir `QuartzConfig`) |
| Pas de `@DisallowConcurrentExecution` | Deux exécutions du job en même temps si l'une dure plus longtemps que l'intervalle | Annoter la classe du job Quartz |
| Mêmes paramètres à chaque lancement | `JobInstanceAlreadyCompleteException` dès la 2e échéance | Un paramètre qui change : `launchTime` |
| Reader sans `@StepScope` | La 2e échéance ne lit rien | `@StepScope` sur le reader |
| Plusieurs instances de l'application avec `RAMJobStore` | Le job s'exécute une fois **par instance** | Stockage JDBC + mode cluster, ou un seul serveur qui planifie |
| Dimanche = 0 (habitude Unix) | Le job tourne le mauvais jour | Utiliser les noms : `SUN`, `MON`... |
| `setWaitForJobsToCompleteOnShutdown(false)` (valeur par défaut) | Arrêt pendant un job : exécution bloquée en `STARTED` dans `BATCH_JOB_EXECUTION` | `setWaitForJobsToCompleteOnShutdown(true)` |

---

## 12. Pour aller plus loin

1. **Ajouter un 2e trigger** au même JobDetail, par exemple `0 0 22 ? * MON-FRI` en plus des 5 minutes : `setTriggers(trigger1, trigger2)` sur le `SchedulerFactoryBean`.
2. **Planifier par programme** : injecter le `Scheduler` Quartz (`schedulerFactoryBean.getScheduler()`) et utiliser son API : `pauseJob`, `resumeJob`, `triggerJob` (déclencher tout de suite), `rescheduleJob` (changer le cron sans redémarrer).
3. **Passer des données au job Quartz** : `JobDetailFactoryBean.setJobDataAsMap(...)`. Elles arrivent dans `context.getMergedJobDataMap()` et peuvent devenir des paramètres du job Spring Batch.
4. **Stockage JDBC et mode cluster** : créer les tables `QRTZ_*` (script fourni dans le jar de Quartz : `org/quartz/impl/jdbcjobstore/tables_h2.sql`, et un script par base de données), donner une `DataSource` au `SchedulerFactoryBean`, activer `org.quartz.jobStore.isClustered=true`, lancer deux instances (sur deux ports) et constater qu'une seule exécute chaque échéance.
5. **Écouter Quartz** : un `JobListener` ou un `TriggerListener` pour tracer chaque déclenchement, chaque échéance manquée...

Documentation officielle de Quartz 2.3 : <https://www.quartz-scheduler.org/documentation/quartz-2.3.0/>
