# Hello World Spring Batch

Un exemple minimal et commenté pour comprendre **Spring Batch 5** avec **Spring Boot 3**.

Le job affiche un message, puis transforme une liste de noms en salutations, en les traitant par paquets.

- Java 21
- Spring Boot 3.5.x (qui embarque Spring Batch 5.2.x)
- Base H2 en mémoire (pour les métadonnées Spring Batch)
- Maven

---

## Sommaire

1. [Lancer l'exemple](#1-lancer-lexemple)
2. [Qu'est-ce que Spring Batch ?](#2-quest-ce-que-spring-batch-)
3. [Les concepts clés](#3-les-concepts-clés)
4. [Structure du projet](#4-structure-du-projet)
5. [Le code expliqué en détail](#5-le-code-expliqué-en-détail)
6. [Ce qui se passe à l'exécution](#6-ce-qui-se-passe-à-lexécution)
7. [Le JobRepository et les tables de métadonnées](#7-le-jobrepository-et-les-tables-de-métadonnées)
8. [Le test](#8-le-test)
9. [Pièges classiques](#9-pièges-classiques)
10. [Pour aller plus loin](#10-pour-aller-plus-loin)

---

## 1. Lancer l'exemple

```bash
# Lancer le job
mvn spring-boot:run

# Lancer les tests
mvn test

# Construire un jar exécutable puis le lancer
mvn package
java -jar target/hello-batch-0.0.1-SNAPSHOT.jar
```

Sortie attendue (logs simplifiés) :

```
Job: [SimpleJob: [name=helloJob]] launched with the following parameters: [{}]
Executing step: [helloStep]
>>> Hello World from Spring Batch!
Executing step: [greetStep]
--- writing chunk of 2 item(s)
    Hello, ALICE!
    Hello, BOB!
--- writing chunk of 2 item(s)
    Hello, CHARLIE!
    Hello, DIANE!
--- writing chunk of 1 item(s)
    Hello, EVE!
Job: [SimpleJob: [name=helloJob]] completed ... and the following status: [COMPLETED]
```

---

## 2. Qu'est-ce que Spring Batch ?

Spring Batch est un framework de **traitement par lots** : des traitements sans interface utilisateur, souvent lancés par un ordonnanceur (cron, Control-M, Kubernetes CronJob...), qui manipulent de gros volumes de données.

Exemples typiques :
- importer un fichier CSV de 2 millions de lignes dans une base ;
- calculer chaque nuit les factures de tous les clients ;
- exporter des données vers un partenaire ;
- migrer des données d'un système à un autre.

Ce que Spring Batch apporte par rapport à une simple boucle `for` :

| Besoin | Ce que fournit Spring Batch |
|---|---|
| Ne pas tout charger en mémoire | Lecture élément par élément, écriture par paquets (*chunks*) |
| Ne pas tout perdre en cas d'erreur | Une transaction par chunk : seuls les éléments du chunk en cours sont annulés |
| Reprendre là où on s'est arrêté | L'état de chaque exécution est enregistré en base (`JobRepository`) |
| Tolérer quelques lignes invalides | *Skip* et *retry* configurables |
| Savoir ce qui s'est passé | Compteurs (lus, écrits, filtrés, commits, rollbacks...) enregistrés en base |
| Lire / écrire des formats courants | Readers et writers prêts à l'emploi : CSV, XML, JSON, JDBC, JPA, Kafka... |

---

## 3. Les concepts clés

```
                 ┌──────────────────────────── Job ─────────────────────────────┐
JobLauncher ───▶ │   Step 1 (Tasklet)   ──▶   Step 2 (Chunk)   ──▶   Step N ... │
                 └──────────────────────────────────────────────────────────────┘
                                                   │
                                   ┌───────────────┼────────────────┐
                                   ▼               ▼                ▼
                              ItemReader ──▶ ItemProcessor ──▶ ItemWriter

            Toutes les exécutions sont enregistrées dans le JobRepository (base de données)
```

| Concept | Rôle |
|---|---|
| **Job** | Le traitement complet. Une suite ordonnée de Steps. |
| **Step** | Une étape du job. Soit une *Tasklet*, soit un traitement *Chunk*. |
| **Tasklet** | Une action unique (supprimer un fichier, appeler un service, afficher un message...). |
| **Chunk** | Le modèle principal : lire, transformer et écrire des éléments par paquets de N, chaque paquet dans une transaction. |
| **ItemReader** | Lit un élément à la fois. Renvoie `null` quand il n'y a plus rien à lire. |
| **ItemProcessor** | (Optionnel) Transforme un élément. Renvoyer `null` filtre l'élément. |
| **ItemWriter** | Écrit une liste d'éléments (un chunk) en une fois. |
| **JobRepository** | Enregistre en base l'état de chaque job et de chaque step. |
| **JobLauncher** | Démarre un job avec des paramètres (`JobParameters`). |
| **JobInstance** | Un job + un jeu de paramètres identifiants. Ex. : « import du 30/09 ». |
| **JobExecution** | Une tentative d'exécution d'une JobInstance. Une instance en échec peut avoir plusieurs exécutions (relances). |
| **StepExecution** | Une exécution d'un step, avec ses compteurs. |

Pour distinguer **JobInstance** et **JobExecution** : si l'import du 30/09 échoue puis réussit à la relance, il y a **1** JobInstance (« import du 30/09 ») et **2** JobExecutions (l'échec et le succès).

---

## 4. Structure du projet

```
.
├── pom.xml                                   Dépendances Maven
├── README.md                                 Ce fichier
└── src
    ├── main/java/com/example/hellobatch
    │   ├── HelloBatchApplication.java        Point d'entrée Spring Boot
    │   └── HelloJobConfig.java               Définition du job et de ses steps
    └── test/java/com/example/hellobatch
        └── HelloJobTest.java                 Test d'intégration du job
```

Il n'y a pas de fichier `application.properties` : la configuration par défaut de Spring Boot suffit.

---

## 5. Le code expliqué en détail

### 5.1 `pom.xml`

```xml
<parent>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-parent</artifactId>
    <version>3.5.16</version>
</parent>
```
Le parent Spring Boot fixe les versions de toutes les dépendances Spring. Spring Boot 3.5.x embarque Spring Batch 5.2.x : aucune dépendance n'a donc besoin de version.

| Dépendance | Pourquoi |
|---|---|
| `spring-boot-starter-batch` | Spring Batch + l'auto-configuration Spring Boot (JobRepository, JobLauncher, lancement automatique du job...) |
| `h2` | Base en mémoire. Avec Spring Boot 3, Spring Batch 5 **a besoin d'une base** pour stocker ses métadonnées. |
| `spring-boot-starter-test` | JUnit 5, AssertJ, `@SpringBootTest` |
| `spring-batch-test` | `@SpringBatchTest`, `JobLauncherTestUtils` |

### 5.2 `HelloBatchApplication.java` : le point d'entrée

```java
@SpringBootApplication
public class HelloBatchApplication {
    public static void main(String[] args) {
        System.exit(SpringApplication.exit(SpringApplication.run(HelloBatchApplication.class, args)));
    }
}
```

Au démarrage, grâce à `spring-boot-starter-batch`, Spring Boot :

1. crée une `DataSource` vers H2 (puisque H2 est dans le classpath) ;
2. crée les **tables de métadonnées** `BATCH_*` dans cette base (automatique pour une base embarquée) ;
3. crée l'infrastructure : `JobRepository`, `JobLauncher`, `PlatformTransactionManager`... ;
4. trouve le bean `Job` (`helloJob`) et **le lance automatiquement** (composant `JobLauncherApplicationRunner`).

Les appels imbriqués de `main` se lisent de l'intérieur vers l'extérieur :
- `SpringApplication.run(...)` démarre le contexte, **et c'est pendant ce démarrage que le job s'exécute** ;
- `SpringApplication.exit(...)` ferme le contexte et calcule un code de sortie (non nul si le job a échoué) ;
- `System.exit(...)` transmet ce code au système, pour que l'ordonnanceur sache si le batch a réussi.

> ⚠️ **Ne pas ajouter `@EnableBatchProcessing`** avec Spring Boot 3. Cette annotation (très présente dans les anciens tutoriels) désactive l'auto-configuration Batch de Spring Boot : le job ne serait plus lancé automatiquement et les tables ne seraient plus créées.

### 5.3 `HelloJobConfig.java` : le job

Une classe `@Configuration` qui déclare les éléments du batch comme des beans Spring (`@Bean`). Spring les crée et les injecte les uns dans les autres.

```
helloJob
 ├── helloStep   (Tasklet : affiche "Hello World")
 └── greetStep   (Chunk   : noms -> "Hello, NOM!" -> console, par paquets de 2)
```

#### Step 1 : `helloStep`, une Tasklet

```java
@Bean
public Step helloStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
    return new StepBuilder("helloStep", jobRepository)
            .tasklet((contribution, chunkContext) -> {
                System.out.println(">>> Hello World from Spring Batch!");
                return RepeatStatus.FINISHED;
            }, transactionManager)
            .build();
}
```

- `JobRepository` et `PlatformTransactionManager` sont fournis par Spring Boot et injectés en paramètre.
- `new StepBuilder("helloStep", jobRepository)` : le nom du step apparaît dans les logs et dans la table `BATCH_STEP_EXECUTION`. Le `JobRepository` permet au step d'enregistrer son état.
- La tasklet est écrite en **lambda**, car `Tasklet` est une interface avec une seule méthode `execute(contribution, chunkContext)` :
  - `contribution` sert à mettre à jour les compteurs du step ;
  - `chunkContext` donne accès au contexte (paramètres du job, etc.).
- La valeur de retour indique à Spring Batch quoi faire ensuite :
  - `RepeatStatus.FINISHED` : terminé, on passe au step suivant ;
  - `RepeatStatus.CONTINUABLE` : Spring Batch rappelle la tasklet (pour faire une boucle).
- L'exécution de la tasklet est entourée d'une transaction (`transactionManager`).

#### Step 2 : `greetStep`, un step « chunk »

```java
@Bean
public Step greetStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                      ListItemReader<String> namesReader) {
    return new StepBuilder("greetStep", jobRepository)
            .<String, String>chunk(2, transactionManager)
            .reader(namesReader)
            .processor(greetingProcessor())
            .writer(consoleWriter())
            .build();
}
```

- `<String, String>` : type des éléments **lus** (un nom), puis type des éléments **écrits** (une salutation).
- `chunk(2, ...)` : taille du chunk (*commit interval*). Spring Batch lit et transforme 2 éléments, les écrit ensemble, puis valide la transaction. En production on utilise plutôt 100, 500 ou 1000 selon le volume.

Déroulement complet avec 5 noms et des chunks de 2 :

```
Chunk 1 : [transaction] read Alice, read Bob         → process ×2 → write [ALICE, BOB]      [commit]
Chunk 2 : [transaction] read Charlie, read Diane     → process ×2 → write [CHARLIE, DIANE]  [commit]
Chunk 3 : [transaction] read Eve, read → null (fin)  → process ×1 → write [EVE]             [commit]
```

Donc 5 lectures, 5 écritures et 3 commits : ce sont exactement les valeurs vérifiées par le test.

Si le chunk 2 échouait, seule sa transaction serait annulée : le chunk 1 resterait validé, et une relance du job pourrait reprendre à partir de là (avec un reader qui sait enregistrer sa position, voir [section 10](#10-pour-aller-plus-loin)).

#### Le reader : `namesReader`

```java
@Bean
@StepScope
public ListItemReader<String> namesReader() {
    return new ListItemReader<>(List.of("Alice", "Bob", "Charlie", "Diane", "Eve"));
}
```

- Un `ItemReader` renvoie un élément à chaque appel de `read()`, puis `null` quand il a terminé : c'est ce `null` qui met fin au step.
- `ListItemReader` lit une liste en mémoire. C'est parfait pour un exemple ; en vrai on lirait un fichier (`FlatFileItemReader`) ou une base (`JdbcCursorItemReader`, `JdbcPagingItemReader`).
- **`@StepScope` est important ici**. `ListItemReader` a un état : il retire les éléments de sa liste au fur et à mesure. Sans `@StepScope`, le bean serait un singleton créé une seule fois pour toute la vie de l'application, et au 2e lancement du job la liste serait déjà vide. Avec `@StepScope`, Spring crée **un nouveau reader à chaque exécution du step**.
- `@StepScope` permet aussi d'injecter des paramètres du job dans le bean, par exemple `@Value("#{jobParameters['fichier']}") String fichier`.

#### Le processor : `greetingProcessor`

```java
private ItemProcessor<String, String> greetingProcessor() {
    return name -> "Hello, " + name.toUpperCase() + "!";
}
```

- Il reçoit **un** élément et renvoie **un** élément transformé. C'est ici que se place la logique métier : validation, conversion, enrichissement...
- S'il renvoie `null`, l'élément est **filtré** : il n'est pas écrit, et le compteur `filterCount` du step augmente.
- Le processor est optionnel.
- Il n'a pas d'état, donc pas besoin d'en faire un bean : une simple méthode privée suffit.

#### Le writer : `consoleWriter`

```java
private ItemWriter<String> consoleWriter() {
    return chunk -> {
        System.out.println("--- writing chunk of " + chunk.size() + " item(s)");
        chunk.forEach(greeting -> System.out.println("    " + greeting));
    };
}
```

- Contrairement au reader et au processor, le writer reçoit **tout le chunk** (un objet `Chunk`, qui contient une liste d'éléments). On peut ainsi écrire efficacement, par exemple avec un seul `INSERT` en batch JDBC pour tous les éléments.
- En vrai on utiliserait `FlatFileItemWriter`, `JdbcBatchItemWriter`, `JpaItemWriter`...

#### Le Job : `helloJob`

```java
@Bean
public Job helloJob(JobRepository jobRepository, Step helloStep, Step greetStep) {
    return new JobBuilder("helloJob", jobRepository)
            .start(helloStep)
            .next(greetStep)
            .build();
}
```

- Les deux steps sont injectés **par nom de paramètre** : `helloStep` et `greetStep` correspondent aux noms des méthodes `@Bean`. Comme il y a deux beans de type `Step`, c'est le nom qui permet à Spring de choisir.
- `start(...).next(...)` : exécution séquentielle. Si un step échoue, le job s'arrête en statut `FAILED` et les steps suivants ne sont pas exécutés.
- Spring Batch permet aussi des flux conditionnels, par exemple `.on("FAILED").to(stepDeSecours)`.

---

## 6. Ce qui se passe à l'exécution

```
mvn spring-boot:run
  │
  ├─ Spring Boot démarre
  │    ├─ crée la DataSource H2 (en mémoire)
  │    ├─ crée les tables BATCH_*
  │    ├─ crée JobRepository, JobLauncher, TransactionManager
  │    └─ crée les beans de HelloJobConfig (helloStep, greetStep, helloJob)
  │
  ├─ JobLauncherApplicationRunner lance helloJob
  │    ├─ JobInstance + JobExecution créées en base         (statut STARTED)
  │    ├─ helloStep : exécute la tasklet                    (StepExecution COMPLETED)
  │    ├─ greetStep : 3 chunks lus / transformés / écrits   (StepExecution COMPLETED)
  │    └─ JobExecution mise à jour                          (statut COMPLETED)
  │
  └─ SpringApplication.exit → code de sortie 0 → la JVM s'arrête
```

Comme H2 est **en mémoire**, la base disparaît à l'arrêt de l'application : chaque lancement repart de zéro.

---

## 7. Le JobRepository et les tables de métadonnées

Spring Batch enregistre tout ce qu'il fait dans ces tables :

| Table | Contenu |
|---|---|
| `BATCH_JOB_INSTANCE` | Une ligne par JobInstance (nom du job + clé calculée à partir des paramètres identifiants) |
| `BATCH_JOB_EXECUTION` | Une ligne par exécution : dates de début/fin, statut, code de sortie |
| `BATCH_JOB_EXECUTION_PARAMS` | Les paramètres de chaque exécution |
| `BATCH_STEP_EXECUTION` | Une ligne par exécution de step : compteurs (read, write, filter, commit, rollback, skip...) |
| `BATCH_JOB_EXECUTION_CONTEXT` / `BATCH_STEP_EXECUTION_CONTEXT` | Données sauvegardées pour pouvoir reprendre (ex. : numéro de la dernière ligne lue) |

Ces tables permettent à Spring Batch de :
- **refuser de relancer** un job déjà terminé avec succès avec les mêmes paramètres (`JobInstanceAlreadyCompleteException`) ;
- **reprendre** un job en échec au step (et à la position) où il s'était arrêté ;
- garder un **historique** consultable.

Avec une vraie base persistante (PostgreSQL, Oracle...), relancer `helloJob` avec les mêmes paramètres (ici aucun) échouerait donc la 2e fois. Deux solutions : passer un paramètre qui change à chaque lancement (par exemple la date), ou ajouter un `RunIdIncrementer` au job (`.incrementer(new RunIdIncrementer())`).

---

## 8. Le test

`src/test/java/com/example/hellobatch/HelloJobTest.java`

```java
@SpringBootTest(properties = "spring.batch.job.enabled=false")
@SpringBatchTest
class HelloJobTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Test
    void jobCompletesAndProcessesAllNames() throws Exception {
        JobExecution execution = jobLauncherTestUtils.launchJob();
        assertThat(execution.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        // ... vérifie les compteurs de greetStep : 5 lus, 5 écrits, 3 commits
    }
}
```

- `@SpringBootTest` démarre le contexte Spring complet, comme l'application.
- `spring.batch.job.enabled=false` **désactive le lancement automatique** du job au démarrage. Sans cela, le job tournerait une première fois au démarrage du contexte de test, puis une seconde fois dans le test : c'est le test qui doit décider quand le lancer.
- `@SpringBatchTest` ajoute des outils au contexte, dont `JobLauncherTestUtils`, qui trouve automatiquement l'unique bean `Job`.
- `launchJob()` lance le job **de façon synchrone** avec des paramètres uniques (un nombre aléatoire). Chaque appel crée donc une nouvelle JobInstance et le test peut être rejoué sans erreur « job déjà terminé ».
- On vérifie ensuite le résultat grâce aux métadonnées : le statut du job et les compteurs du `StepExecution` de `greetStep`.
- `JobLauncherTestUtils` permet aussi de tester **un seul step** : `launchStep("greetStep")`.

---

## 9. Pièges classiques

| Piège | Symptôme | Solution |
|---|---|---|
| `@EnableBatchProcessing` avec Spring Boot 3 | Le job ne se lance plus, erreur « table BATCH_JOB_INSTANCE not found » | Retirer l'annotation et laisser Spring Boot configurer Batch |
| Reader avec état déclaré en singleton | Le 2e lancement du job ne lit rien (0 élément) | `@StepScope` sur le bean reader |
| Relancer un job terminé avec les mêmes paramètres | `JobInstanceAlreadyCompleteException` | Paramètres différents à chaque lancement, ou `RunIdIncrementer` |
| Plusieurs `Job` dans le contexte | Spring Boot refuse de choisir lequel lancer | Préciser `spring.batch.job.name=monJob` |
| Pas de base de données | Erreur au démarrage : aucune `DataSource` | Ajouter une base (H2 pour apprendre) |
| Tutoriels anciens (Spring Batch 4) | `JobBuilderFactory` / `StepBuilderFactory` introuvables | Utiliser `new JobBuilder(nom, jobRepository)` et `new StepBuilder(nom, jobRepository)` |

---

## 10. Pour aller plus loin

Idées pour faire évoluer cet exemple, dans un ordre progressif :

1. **Lire un fichier CSV** avec `FlatFileItemReader` (via `FlatFileItemReaderBuilder`) et le transformer en objets (`record Person(String firstName, String lastName)`).
2. **Écrire en base** avec `JdbcBatchItemWriter`, puis observer les tables `BATCH_*` avec la console H2 (`spring.h2.console.enabled=true`, qui nécessite aussi `spring-boot-starter-web`).
3. **Passer des paramètres au job** (`mvn spring-boot:run -Dspring-boot.run.arguments="fichier=data.csv"`) et les lire avec `@Value("#{jobParameters['fichier']}")` dans un bean `@StepScope`.
4. **Gérer les erreurs** : `.faultTolerant().skip(FlatFileParseException.class).skipLimit(10)` pour ignorer les lignes invalides, `.retry(...)` pour réessayer.
5. **Tester la reprise** : faire échouer le job au milieu, puis le relancer avec les mêmes paramètres et constater qu'il reprend où il s'était arrêté.
6. **Ajouter des listeners** (`JobExecutionListener`, `StepExecutionListener`, `ChunkListener`) pour tracer le début et la fin des traitements.
7. **Flux conditionnels** : `.on("FAILED").to(...)`, ou un `JobExecutionDecider`.
8. **Paralléliser** : step multi-threadé (`.taskExecutor(...)`), partitionnement.

Documentation officielle : <https://docs.spring.io/spring-batch/reference/>
