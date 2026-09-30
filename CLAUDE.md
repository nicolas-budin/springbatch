# CLAUDE.md

Projet d'apprentissage de Spring Batch : un Hello World pédagogique, destiné à être enrichi pas à pas.

## Commandes

```bash
mvn spring-boot:run   # lance le job (helloJob) puis l'application s'arrête
mvn test              # tests (HelloJobTest)
mvn package           # jar exécutable dans target/
```

Toujours lancer `mvn test` **et** `mvn spring-boot:run` après une modification, puis vérifier dans la sortie console que le job se termine en `COMPLETED`.

## Stack (à respecter)

- **Spring Batch 5** (5.2.x) via **Spring Boot 3.5.x**. Ne pas passer à Spring Boot 4 / Spring Batch 6 : c'est un choix explicite de l'utilisateur.
- Java 21, Maven, base H2 en mémoire pour le JobRepository.
- Packages Spring Batch 5 : `org.springframework.batch.core.{Job,Step,JobExecution}`, `org.springframework.batch.item.*`, `org.springframework.batch.repeat.RepeatStatus`. (Les packages de Spring Batch 6, comme `org.springframework.batch.infrastructure.*` ou `org.springframework.batch.core.job.Job`, ne compilent pas ici.)
- API de Spring Batch 5 : `new JobBuilder(nom, jobRepository)`, `new StepBuilder(nom, jobRepository)`, `.chunk(n, transactionManager)`, `.tasklet(t, transactionManager)`. Pas de `JobBuilderFactory` / `StepBuilderFactory` (Spring Batch 4).

## Structure

- `src/main/java/com/example/hellobatch/HelloBatchApplication.java` : point d'entrée Spring Boot.
- `src/main/java/com/example/hellobatch/HelloJobConfig.java` : `helloJob` = `helloStep` (tasklet) puis `greetStep` (chunk de 2 : `ListItemReader` → processor → writer console).
- `src/test/java/com/example/hellobatch/HelloJobTest.java` : lance le job via `JobLauncherTestUtils` et vérifie le statut et les compteurs du step.
- `README.md` : explication détaillée du code et des concepts, en français.

## Conventions

- Le but est **pédagogique** : code simple, commentaires **en français** et détaillés, qui expliquent le *pourquoi* (concepts Spring Batch), pas seulement le *quoi*. Garder ce niveau de commentaire pour tout nouveau code.
- Quand le code change, mettre à jour `README.md` en même temps : extraits de code, sortie attendue, section « Pour aller plus loin ».
- Les messages de commit sont en anglais.

## Pièges connus

- **Pas de `@EnableBatchProcessing`** : cette annotation désactive l'auto-configuration Batch de Spring Boot 3 (plus de lancement automatique du job, plus de création des tables `BATCH_*`).
- Les readers qui gardent un état doivent être des beans `@StepScope`, sinon le 2e lancement du job ne lit rien.
- Les tests utilisent `spring.batch.job.enabled=false` pour que le job ne soit pas lancé automatiquement au démarrage du contexte.
- S'il y a plusieurs beans `Job`, Spring Boot exige `spring.batch.job.name` pour savoir lequel lancer.
