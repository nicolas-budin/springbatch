# Hello World Spring Batch

Un exemple minimal et commenté pour comprendre **Spring Batch 3** avec **Spring Boot 1.5**.

Le job affiche un message, puis transforme une liste de noms en salutations, en les traitant par paquets.

- Java 8 (le code compile en Java 8, mais tourne aussi sur un JDK récent : voir [section 1](#1-lancer-lexemple))
- Spring Boot 1.5.22 (qui embarque Spring Batch 3.0.10 et Spring Framework 4.3)
- Base H2 en mémoire (pour les métadonnées Spring Batch)
- Maven

> Spring Batch 3 est une version ancienne (2014-2017), qui n'est plus maintenue. Ce projet l'utilise pour aider à lire du code existant écrit avec cette version. Pour un nouveau projet, on utiliserait Spring Batch 5 : la [section 11](#11-différences-avec-spring-batch-5) liste les différences.

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
11. [Différences avec Spring Batch 5](#11-différences-avec-spring-batch-5)

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

Ces commandes fonctionnent avec **Java 8** comme avec un **JDK récent** (testé avec Java 25). Avec un JDK 9 ou plus, le profil Maven `jdk9-et-plus` s'active tout seul et ajoute les options JVM nécessaires (voir [5.1](#51-pomxml)).

Maven n'est pas installé ? On peut tout lancer dans Docker, par exemple avec un vrai Java 8 :

```bash
docker run --rm -v "$PWD":/app -w /app maven:3.9-eclipse-temurin-8 mvn spring-boot:run
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
| Lire / écrire des formats courants | Readers et writers prêts à l'emploi : CSV, XML, JDBC, JPA, JMS... |

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
    <version>1.5.22.RELEASE</version>
</parent>

<properties>
    <java.version>1.8</java.version>
</properties>
```
Le parent Spring Boot fixe les versions de toutes les dépendances Spring. Spring Boot 1.5.x embarque Spring Batch 3.0.x : les dépendances Spring n'ont donc pas besoin de version. `java.version` à `1.8` : le code doit rester compatible Java 8 (lambdas autorisées, mais pas de `var`, `List.of` ni `record`).

| Dépendance | Pourquoi |
|---|---|
| `spring-boot-starter-batch` | Spring Batch + l'auto-configuration Spring Boot (création des tables, lancement automatique du job...) |
| `h2` | Base en mémoire. Spring Batch **a besoin d'une base** pour stocker ses métadonnées. |
| `javax.annotation-api` | Les annotations `@PostConstruct`, `@Resource`... Elles faisaient partie du JDK jusqu'à Java 10. Voir ci-dessous. |
| `spring-boot-starter-test` | JUnit 4, AssertJ, `@SpringBootTest` |
| `spring-batch-test` | `JobLauncherTestUtils` |

**Faire tourner Spring Batch 3 sur un JDK récent.** Spring Batch 3 a été écrit pour Java 6 à 8. Sur un JDK récent, deux problèmes apparaissent, et le `pom.xml` les corrige :

1. **`javax.annotation` a disparu du JDK (Java 11).** Spring Boot 1.5 crée les tables `BATCH_*` dans une méthode `@PostConstruct`. Sans la dépendance `javax.annotation-api`, Spring ne reconnaît plus l'annotation et **ignore la méthode sans rien dire**. Le job échoue alors avec `Table "BATCH_JOB_INSTANCE" not found`.
2. **Le JDK interdit l'accès à ses classes internes (Java 16).** Spring 4.3 (proxies CGLIB pour `@Configuration` et `@StepScope`) et XStream (qui enregistre les `ExecutionContext` en base) accèdent par réflexion aux classes internes du JDK. Il faut l'autoriser avec des options `--add-opens`. Elles sont ajoutées par le profil Maven `jdk9-et-plus`, qui s'active tout seul avec un JDK 9 ou plus (Java 8 ne connaît pas cette option et refuserait de démarrer), et par une entrée `Add-Opens` dans le manifeste du jar, pour `java -jar`.

Sans l'option pour `java.lang`, par exemple, l'erreur ressemble à `InaccessibleObjectException: Unable to make ... accessible: module java.base does not "opens java.lang"`.

### 5.2 `HelloBatchApplication.java` : le point d'entrée

```java
@SpringBootApplication
@EnableBatchProcessing
public class HelloBatchApplication {
    public static void main(String[] args) {
        System.exit(SpringApplication.exit(SpringApplication.run(HelloBatchApplication.class, args)));
    }
}
```

Au démarrage, deux mécanismes travaillent ensemble :

1. `@EnableBatchProcessing` (Spring Batch) crée l'infrastructure : `JobRepository`, `JobLauncher`, `PlatformTransactionManager`, ainsi que les fabriques **`JobBuilderFactory`** et **`StepBuilderFactory`** utilisées pour construire le job ;
2. l'auto-configuration de Spring Boot (`spring-boot-starter-batch`) :
   - crée une `DataSource` vers H2 (puisque H2 est dans le classpath) ;
   - crée les **tables de métadonnées** `BATCH_*` dans cette base (automatique pour une base embarquée) ;
   - trouve le bean `Job` (`helloJob`) et **le lance automatiquement** (composant `JobLauncherCommandLineRunner`).

Les appels imbriqués de `main` se lisent de l'intérieur vers l'extérieur :
- `SpringApplication.run(...)` démarre le contexte, **et c'est pendant ce démarrage que le job s'exécute** ;
- `SpringApplication.exit(...)` ferme le contexte et calcule un code de sortie (non nul si le job a échoué) ;
- `System.exit(...)` transmet ce code au système, pour que l'ordonnanceur sache si le batch a réussi.

> ⚠️ Avec Spring Boot 1.x, **`@EnableBatchProcessing` est obligatoire** : sans elle, pas de `JobBuilderFactory` / `StepBuilderFactory`. C'est l'inverse avec Spring Boot 3 / Spring Batch 5, où cette annotation désactive l'auto-configuration. Il faut garder ça en tête quand on lit du code écrit pour une autre version.

### 5.3 `HelloJobConfig.java` : le job

Une classe `@Configuration` qui déclare les éléments du batch comme des beans Spring (`@Bean`). Spring les crée et les injecte les uns dans les autres.

```
helloJob
 ├── helloStep   (Tasklet : affiche "Hello World")
 └── greetStep   (Chunk   : noms -> "Hello, NOM!" -> console, par paquets de 2)
```

#### Les fabriques

```java
@Autowired
private JobBuilderFactory jobBuilderFactory;

@Autowired
private StepBuilderFactory stepBuilderFactory;
```

Ces deux beans sont fournis par `@EnableBatchProcessing`. Ils connaissent déjà le `JobRepository` (et, pour les steps, le transaction manager) : `stepBuilderFactory.get("nom")` renvoie un constructeur de step déjà branché sur l'infrastructure. On les injecte ici dans des **champs** avec `@Autowired`, le style le plus courant dans le code de cette époque.

#### Step 1 : `helloStep`, une Tasklet

```java
@Bean
public Step helloStep() {
    return stepBuilderFactory.get("helloStep")
            .tasklet((contribution, chunkContext) -> {
                System.out.println(">>> Hello World from Spring Batch!");
                return RepeatStatus.FINISHED;
            })
            .build();
}
```

- `stepBuilderFactory.get("helloStep")` : le nom du step apparaît dans les logs et dans la table `BATCH_STEP_EXECUTION`.
- La tasklet est écrite en **lambda** (possible depuis Java 8), car `Tasklet` est une interface avec une seule méthode `execute(contribution, chunkContext)` :
  - `contribution` sert à mettre à jour les compteurs du step ;
  - `chunkContext` donne accès au contexte (paramètres du job, etc.).
- La valeur de retour indique à Spring Batch quoi faire ensuite :
  - `RepeatStatus.FINISHED` : terminé, on passe au step suivant ;
  - `RepeatStatus.CONTINUABLE` : Spring Batch rappelle la tasklet (pour faire une boucle).
- L'exécution de la tasklet est entourée d'une transaction, gérée par le transaction manager que la fabrique fournit implicitement.

#### Step 2 : `greetStep`, un step « chunk »

```java
@Bean
public Step greetStep(ListItemReader<String> namesReader) {
    return stepBuilderFactory.get("greetStep")
            .<String, String>chunk(2)
            .reader(namesReader)
            .processor(greetingProcessor())
            .writer(consoleWriter())
            .build();
}
```

- `<String, String>` : type des éléments **lus** (un nom), puis type des éléments **écrits** (une salutation).
- `chunk(2)` : taille du chunk (*commit interval*). Spring Batch lit et transforme 2 éléments, les écrit ensemble, puis valide la transaction. En production on utilise plutôt 100, 500 ou 1000 selon le volume.

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
    return new ListItemReader<>(Arrays.asList("Alice", "Bob", "Charlie", "Diane", "Eve"));
}
```

- Un `ItemReader` renvoie un élément à chaque appel de `read()`, puis `null` quand il a terminé : c'est ce `null` qui met fin au step.
- `ListItemReader` lit une liste en mémoire. C'est parfait pour un exemple ; en vrai on lirait un fichier (`FlatFileItemReader`) ou une base (`JdbcCursorItemReader`, `JdbcPagingItemReader`).
- `Arrays.asList` et non `List.of`, qui n'existe qu'à partir de Java 9.
- **`@StepScope` est important ici**. `ListItemReader` a un état : il retire les éléments de sa liste au fur et à mesure. Sans `@StepScope`, le bean serait un singleton créé une seule fois pour toute la vie de l'application, et au 2e lancement du job la liste serait déjà vide. Avec `@StepScope`, Spring crée **un nouveau reader à chaque exécution du step**.
- `@StepScope` permet aussi d'injecter des paramètres du job dans le bean, par exemple `@Value("#{jobParameters['fichier']}") String fichier`.
- En configuration XML (fréquente en Spring Batch 3), l'équivalent est l'attribut `scope="step"` sur le `<bean>`.

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
    return items -> {
        System.out.println("--- writing chunk of " + items.size() + " item(s)");
        items.forEach(greeting -> System.out.println("    " + greeting));
    };
}
```

- Contrairement au reader et au processor, le writer reçoit **tout le chunk**, sous la forme d'une `List` : la signature est `write(List<? extends T> items)`. On peut ainsi écrire efficacement, par exemple avec un seul `INSERT` en batch JDBC pour tous les éléments.
- En vrai on utiliserait `FlatFileItemWriter`, `JdbcBatchItemWriter`, `JpaItemWriter`...

#### Le Job : `helloJob`

```java
@Bean
public Job helloJob(Step helloStep, Step greetStep) {
    return jobBuilderFactory.get("helloJob")
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
  │    ├─ @EnableBatchProcessing : JobRepository, JobLauncher, TransactionManager,
  │    │                           JobBuilderFactory, StepBuilderFactory
  │    └─ crée les beans de HelloJobConfig (helloStep, greetStep, helloJob)
  │
  ├─ JobLauncherCommandLineRunner lance helloJob
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
| `BATCH_JOB_EXECUTION_CONTEXT` / `BATCH_STEP_EXECUTION_CONTEXT` | Données sauvegardées pour pouvoir reprendre (ex. : numéro de la dernière ligne lue). En Spring Batch 3, elles sont converties en JSON par la bibliothèque XStream. |

Ces tables permettent à Spring Batch de :
- **refuser de relancer** un job déjà terminé avec succès avec les mêmes paramètres (`JobInstanceAlreadyCompleteException`) ;
- **reprendre** un job en échec au step (et à la position) où il s'était arrêté ;
- garder un **historique** consultable.

Avec une vraie base persistante (PostgreSQL, Oracle...), relancer `helloJob` avec les mêmes paramètres (ici aucun) échouerait donc la 2e fois. Deux solutions : passer un paramètre qui change à chaque lancement (par exemple la date), ou ajouter un `RunIdIncrementer` au job (`.incrementer(new RunIdIncrementer())`).

---

## 8. Le test

`src/test/java/com/example/hellobatch/HelloJobTest.java`

```java
@RunWith(SpringRunner.class)
@SpringBootTest(properties = "spring.batch.job.enabled=false")
public class HelloJobTest {

    @TestConfiguration
    static class BatchTestConfig {
        @Bean
        public JobLauncherTestUtils jobLauncherTestUtils() {
            return new JobLauncherTestUtils();
        }
    }

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Test
    public void jobCompletesAndProcessesAllNames() throws Exception {
        JobExecution execution = jobLauncherTestUtils.launchJob();
        assertThat(execution.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        // ... vérifie les compteurs de greetStep : 5 lus, 5 écrits, 3 commits
    }
}
```

- C'est un test **JUnit 4** (celui fourni par Spring Boot 1.5) : `@RunWith(SpringRunner.class)` branche Spring sur JUnit, et la classe et les méthodes de test doivent être `public`.
- `@SpringBootTest` démarre le contexte Spring complet, comme l'application.
- `spring.batch.job.enabled=false` **désactive le lancement automatique** du job au démarrage. Sans cela, le job tournerait une première fois au démarrage du contexte de test, puis une seconde fois dans le test : c'est le test qui doit décider quand le lancer.
- Le bean `JobLauncherTestUtils` est **déclaré à la main** dans une `@TestConfiguration` (l'annotation `@SpringBatchTest`, qui le fait automatiquement, n'existe qu'à partir de Spring Batch 4.1). Ses setters sont `@Autowired` : il reçoit l'unique bean `Job`, le `JobLauncher` et le `JobRepository`.
- `launchJob()` lance le job **de façon synchrone** avec des paramètres uniques (un nombre aléatoire). Chaque appel crée donc une nouvelle JobInstance et le test peut être rejoué sans erreur « job déjà terminé ».
- On vérifie ensuite le résultat grâce aux métadonnées : le statut du job et les compteurs du `StepExecution` de `greetStep`.
- `JobLauncherTestUtils` permet aussi de tester **un seul step** : `launchStep("greetStep")`.

---

## 9. Pièges classiques

| Piège | Symptôme | Solution |
|---|---|---|
| Oublier `@EnableBatchProcessing` (Spring Boot 1.x) | Aucun bean `JobBuilderFactory` / `StepBuilderFactory` au démarrage | Ajouter l'annotation sur une classe `@Configuration` |
| Reader avec état déclaré en singleton | Le 2e lancement du job ne lit rien (0 élément) | `@StepScope` sur le bean reader (ou `scope="step"` en XML) |
| Méthode `@Bean @StepScope` qui déclare `ItemReader<T>` en retour | `ReaderNotOpenException` : le proxy n'implémente pas `ItemStream`, donc `open()` n'est jamais appelé | Déclarer le type concret (`FlatFileItemReader<T>`) ou `ItemStreamReader<T>` |
| Relancer un job terminé avec les mêmes paramètres | `JobInstanceAlreadyCompleteException` | Paramètres différents à chaque lancement, ou `RunIdIncrementer` |
| Plusieurs `Job` dans le contexte | Spring Boot les lance **tous** au démarrage | Préciser `spring.batch.job.names=monJob` (avec un « s » en Spring Boot 1.x) |
| Pas de base de données | Erreur au démarrage : aucune `DataSource` | Ajouter une base (H2 pour apprendre) |
| JDK 11+ sans `javax.annotation-api` | `Table "BATCH_JOB_INSTANCE" not found` | Ajouter la dépendance (voir [5.1](#51-pomxml)) |
| JDK 16+ sans `--add-opens` | `InaccessibleObjectException ... does not "opens java.lang"` | Options `--add-opens` (voir [5.1](#51-pomxml)) |
| Code Java 9+ (`List.of`, `var`, `record`...) | Erreur de compilation avec `java.version` 1.8 | Équivalents Java 8 : `Arrays.asList`, types explicites, classes classiques |

---

## 10. Pour aller plus loin

Idées pour faire évoluer cet exemple, dans un ordre progressif :

1. **Lire un fichier CSV** avec `FlatFileItemReader` et le transformer en objets (une classe `Person` avec `firstName` et `lastName`). En Spring Batch 3, il n'y a pas de `FlatFileItemReaderBuilder` : on assemble soi-même un `FlatFileItemReader`, un `DefaultLineMapper`, un `DelimitedLineTokenizer` et un `BeanWrapperFieldSetMapper`.
2. **Écrire en base** avec `JdbcBatchItemWriter`, puis observer les tables `BATCH_*` avec la console H2 (`spring.h2.console.enabled=true`, qui nécessite aussi `spring-boot-starter-web`).
3. **Passer des paramètres au job** (`mvn spring-boot:run -Drun.arguments="fichier=data.csv"`) et les lire avec `@Value("#{jobParameters['fichier']}")` dans un bean `@StepScope`.
4. **Gérer les erreurs** : `.faultTolerant().skip(FlatFileParseException.class).skipLimit(10)` pour ignorer les lignes invalides, `.retry(...)` pour réessayer.
5. **Tester la reprise** : faire échouer le job au milieu, puis le relancer avec les mêmes paramètres et constater qu'il reprend où il s'était arrêté.
6. **Ajouter des listeners** (`JobExecutionListener`, `StepExecutionListener`, `ChunkListener`) pour tracer le début et la fin des traitements. Un reader peut lui-même implémenter `StepExecutionListener` : sa méthode `beforeStep(StepExecution)` est appelée avant la première lecture.
7. **Écrire le même job en XML** (`<batch:job>`, `<batch:step>`, `<batch:tasklet>`, `<batch:chunk reader="..." commit-interval="2"/>`), le style dominant dans les projets Spring Batch 2 et 3, et le charger avec `@ImportResource`.
8. **Flux conditionnels** : `.on("FAILED").to(...)`, ou un `JobExecutionDecider`.
9. **Paralléliser** : step multi-threadé (`.taskExecutor(...)`), partitionnement.

Documentation officielle de Spring Batch 3 : <https://docs.spring.io/spring-batch/docs/3.0.x/reference/html/>

---

## 11. Différences avec Spring Batch 5

Ce tableau aide à lire du code écrit avec une version ou l'autre.

| | Spring Batch 3 (ce projet) | Spring Batch 5 |
|---|---|---|
| Spring Boot / Java | Boot 1.5, Java 8 | Boot 3, Java 17+ |
| `@EnableBatchProcessing` | **Obligatoire** | **À éviter** avec Spring Boot (désactive l'auto-configuration) |
| Construire un step | `stepBuilderFactory.get("nom")` | `new StepBuilder("nom", jobRepository)` |
| Construire un job | `jobBuilderFactory.get("nom")` | `new JobBuilder("nom", jobRepository)` |
| Transaction manager | Implicite : `.chunk(2)`, `.tasklet(t)` | Explicite : `.chunk(2, transactionManager)`, `.tasklet(t, transactionManager)` |
| Signature du writer | `write(List<? extends T> items)` | `write(Chunk<? extends T> chunk)` |
| Builders de readers / writers | Aucun : on assemble les objets à la main | `FlatFileItemReaderBuilder`, `JdbcBatchItemWriterBuilder`... |
| Lancement automatique | `JobLauncherCommandLineRunner`, tous les jobs, `spring.batch.job.names` | `JobLauncherApplicationRunner`, un seul job, `spring.batch.job.name` |
| Tests | JUnit 4, `JobLauncherTestUtils` déclaré à la main | JUnit 5, `@SpringBatchTest` |
| Sérialisation des `ExecutionContext` | XStream | Jackson / Java |
| Configuration XML | Très répandue | Toujours supportée, mais rare |
