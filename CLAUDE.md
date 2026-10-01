# CLAUDE.md

Projet d'apprentissage de Spring Batch : un Hello World pédagogique, destiné à être enrichi pas à pas.

## Commandes

```bash
mvn spring-boot:run   # démarre l'application : helloJob tourne toutes les 5 minutes, ne s'arrête pas seule (Ctrl+C)
mvn test              # tests (HelloJobTest, HelloJobSchedulerTest)
mvn package           # jar exécutable dans target/
```

Toujours lancer `mvn test` **et** `mvn spring-boot:run` après une modification (en Java 8 **et** en Java 25, voir ci-dessous), puis vérifier dans la sortie console que le job se termine en `COMPLETED`. Pour ne pas attendre 5 minutes : `java -jar target/hello-batch-0.0.1-SNAPSHOT.jar "--hello.scheduler.cron=*/10 * * * * ?"` (avec `timeout`), et vérifier au moins deux lancements successifs.

## Stack (à respecter)

- **Spring Batch 3** (3.0.10) via **Spring Boot 1.5.22**. Choix explicite de l'utilisateur, pour comprendre un autre programme écrit en Spring Batch 3 (le projet était en Spring Batch 5 avant ; la version 5 reste dans l'historique git). Ne pas remonter de version sans qu'il le demande.
- Code **Java 8** (`java.version` 1.8) : pas de `var`, `List.of`, `record`, `Optional.orElseThrow()` sans argument... Le projet doit aussi tourner sur le JDK récent de la machine (Java 25) : profil Maven `jdk9-et-plus` (`--add-opens`), entrée `Add-Opens` du manifeste, dépendance `javax.annotation-api`.
- Maven, base H2 en mémoire pour le JobRepository. `spring-boot-starter-web` (Tomcat, port 8080) n'est là que pour la console H2 (`/h2-console`, JDBC URL `jdbc:h2:mem:testdb`, user `sa`) ; pour la vérifier dans Docker, `curl http://localhost:8080/h2-console/` doit répondre 200. Maven n'est pas installé en local : lancer les commandes dans Docker, par exemple `docker run --rm -v "$PWD":/app -v hellobatch-m2:/root/.m2 -w /app maven:3.9-eclipse-temurin-8 mvn test` (et vérifier aussi avec l'image `maven:3.9-eclipse-temurin-25`).
- API de Spring Batch 3 : `@EnableBatchProcessing` + `JobBuilderFactory` / `StepBuilderFactory` (`stepBuilderFactory.get(nom)`), `.chunk(n)` et `.tasklet(t)` sans transaction manager, `ItemWriter.write(List<? extends T>)`. Pas de `new JobBuilder(nom, jobRepository)` ni de `Chunk` (Spring Batch 5), pas de `FlatFileItemReaderBuilder` & co (Spring Batch 4).
- Tests en **JUnit 4** : `@RunWith(SpringRunner.class)`, classe et méthodes `public`, `JobLauncherTestUtils` déclaré en `@Bean` dans une `@TestConfiguration` (pas de `@SpringBatchTest`).

## Structure

- `src/main/java/com/example/hellobatch/HelloBatchApplication.java` : point d'entrée Spring Boot (plus de `System.exit` : l'application tourne en continu).
- `src/main/java/com/example/hellobatch/HelloJobConfig.java` : `helloJob` = `helloStep` (tasklet) puis `greetStep` (chunk de 2 : `JdbcPagingItemReader` sur la table `PERSON`, pages de 2 = taille du chunk → processor → writer console).
- `src/main/resources/schema.sql` et `data.sql` : table `PERSON` et ses 5 noms, exécutés par Spring Boot au démarrage. Doivent rester rejouables (`CREATE TABLE IF NOT EXISTS`, `MERGE INTO`) : les contextes de test partagent la même base H2.
- `src/main/java/com/example/hellobatch/HelloJobScheduler.java` : `launch()` → `jobLauncher.run(helloJob, {launchTime})` (sans dépendance à Quartz, appelée directement par les tests).
- `src/main/java/com/example/hellobatch/QuartzConfig.java` : Quartz configuré à la main (Spring Boot 1.5 n'a pas d'auto-configuration Quartz) : `JobDetailFactoryBean` (`HelloQuartzJob`), `CronTriggerFactoryBean` (`${hello.scheduler.cron}`), `SchedulerFactoryBean` en `RAMJobStore` avec une `AutowiringSpringBeanJobFactory` (injection des beans dans les jobs Quartz). Désactivable par `hello.scheduler.enabled=false`.
- `src/main/java/com/example/hellobatch/HelloQuartzJob.java` : `QuartzJobBean` `@DisallowConcurrentExecution` qui appelle `HelloJobScheduler.launch()`.
- `src/main/resources/application.properties` : `spring.batch.job.enabled=false` (pas de lancement au démarrage), `hello.scheduler.cron=0 */5 * * * ?` (format Quartz), `JdbcTemplate` en DEBUG (requêtes SQL de Spring Batch dans les logs).
- `src/test/java/com/example/hellobatch/HelloJobTest.java` : lance le job via `JobLauncherTestUtils` et vérifie le statut et les compteurs du step.
- `src/test/java/com/example/hellobatch/HelloJobSchedulerTest.java` : appelle deux fois `HelloJobScheduler.launch()` et vérifie que les deux lancements réussissent.
- `README.md` : explication détaillée du code et des concepts, en français.

## Conventions

- Le but est **pédagogique** : code simple, commentaires **en français** et détaillés, qui expliquent le *pourquoi* (concepts Spring Batch), pas seulement le *quoi*. Garder ce niveau de commentaire pour tout nouveau code.
- Quand le code change, mettre à jour `README.md` en même temps : extraits de code, sortie attendue, section « Pour aller plus loin ».
- Les messages de commit sont en anglais.

## Pièges connus

- **`@EnableBatchProcessing` est obligatoire** (sur `HelloBatchApplication`) : c'est lui qui fournit `JobBuilderFactory` / `StepBuilderFactory`. (C'est l'inverse en Spring Batch 5.)
- Les readers qui gardent un état doivent être des beans `@StepScope`, sinon le 2e lancement du job ne lit rien.
- Les tests utilisent `spring.batch.job.enabled=false` et `hello.scheduler.enabled=false` : le job n'est lancé ni au démarrage du contexte ni par Quartz, seulement par le test.
- Chaque lancement par Quartz doit avoir des paramètres uniques (`launchTime`), sinon `JobInstanceAlreadyCompleteException` au 2e passage.
- Le cron Quartz a 6 champs (secondes en tête) et exige `?` dans un des deux champs « jour » : `0 */5 * * * ?`. Le format `@Scheduled` (`0 */5 * * * *`) empêche le démarrage (`ParseException`).
- Quartz 2.3.2 a une version explicite dans le `pom.xml` (non gérée par Boot 1.5), avec `c3p0`, `mchange-commons-java` et `HikariCP-java7` exclus (inutiles en `RAMJobStore`).
- S'il y a plusieurs beans `Job`, Spring Boot 1.5 les lance **tous** ; `spring.batch.job.names` (avec un « s ») restreint la liste.
- Sur JDK 11+, sans `javax.annotation-api`, les tables `BATCH_*` ne sont pas créées (le `@PostConstruct` de Spring Boot est ignoré sans erreur) : `Table "BATCH_JOB_INSTANCE" not found`.
- Sur JDK 16+, une `InaccessibleObjectException ... does not "opens ..."` signifie qu'il manque un package dans les `--add-opens` du `pom.xml` (à ajouter à la fois dans le profil `jdk9-et-plus` et dans l'entrée `Add-Opens` du manifeste).
