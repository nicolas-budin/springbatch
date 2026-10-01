# Hello World Spring Batch

Un exemple minimal et commenté pour comprendre **Spring Batch 3** avec **Spring Boot 1.5**.

Le job affiche un message, puis lit une liste de noms dans une table de la base (JDBC) et la transforme en salutations, en les traitant par paquets. Il est lancé **toutes les 5 minutes** par l'ordonnanceur Quartz.

- Java 8 (le code compile en Java 8, mais tourne aussi sur un JDK récent : voir [section 1](#1-lancer-lexemple))
- Spring Boot 1.5.22 (qui embarque Spring Batch 3.0.10 et Spring Framework 4.3)
- Base H2 en mémoire (pour les métadonnées Spring Batch et la table lue par le job), consultable dans le navigateur avec la console H2 : http://localhost:8080/h2-console
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
# Lancer l'application : le job tourne toutes les 5 minutes (à 12:00, 12:05...), Ctrl+C pour arrêter
mvn spring-boot:run

# Lancer les tests
mvn test

# Construire un jar exécutable puis le lancer
mvn package
java -jar target/hello-batch-0.0.1-SNAPSHOT.jar

# Pour tester sans attendre : lancer le job toutes les 10 secondes
java -jar target/hello-batch-0.0.1-SNAPSHOT.jar "--hello.scheduler.cron=*/10 * * * * ?"
```

L'application **ne s'arrête plus d'elle-même** : elle attend le prochain lancement. Au démarrage, rien ne se passe avant la première échéance (par exemple 12:05 si on démarre à 12:03).

Pendant qu'elle tourne, on peut consulter la base dans le navigateur, sur **http://localhost:8080/h2-console** :

| Champ | Valeur |
|---|---|
| JDBC URL | `jdbc:h2:mem:testdb` (**pas** la valeur préremplie `jdbc:h2:~/test`, qui désigne une autre base : erreur `Database "..." not found`) |
| User Name | `sa` |
| Password | *(vide)* |

Les requêtes utiles sont dans la [section 7](#consulter-la-base-avec-la-console-h2).

Ces commandes fonctionnent avec **Java 8** comme avec un **JDK récent** (testé avec Java 25). Avec un JDK 9 ou plus, le profil Maven `jdk9-et-plus` s'active tout seul et ajoute les options JVM nécessaires (voir [5.1](#51-pomxml)).

Maven n'est pas installé ? On peut tout lancer dans Docker, par exemple avec un vrai Java 8 :

```bash
docker run --rm -v "$PWD":/app -w /app maven:3.9-eclipse-temurin-8 mvn spring-boot:run
```

Sortie attendue à chaque lancement (logs simplifiés) :

```
Job: [SimpleJob: [name=helloJob]] launched with the following parameters: [{launchTime=1790765500008}]
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

Les logs du job indiquent le thread `[schedulerFactoryBean_Worker-1]` (puis `-2`, `-3`...) : c'est un thread du pool de Quartz, et non plus `[main]`.

Entre ces lignes, on voit aussi les requêtes SQL que Spring Batch exécute sur ses tables (`Executing prepared SQL statement [...]`), voir [section 7](#7-le-jobrepository-et-les-tables-de-métadonnées).

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
├── SCHEDULER.md                              Le scheduler Quartz expliqué en détail
└── src
    ├── main/java/com/example/hellobatch
    │   ├── HelloBatchApplication.java        Point d'entrée Spring Boot
    │   ├── HelloJobConfig.java               Définition du job et de ses steps
    │   ├── HelloJobScheduler.java            Lance une exécution du job (launch())
    │   ├── HelloQuartzJob.java               Job Quartz : appelé à chaque échéance, appelle launch()
    │   └── QuartzConfig.java                 Configuration de Quartz (JobDetail, Trigger cron, Scheduler)
    ├── main/resources
    │   ├── application.properties            Pas de lancement au démarrage, fréquence (cron), logs SQL
    │   ├── schema.sql                        Création de la table PERSON (exécuté au démarrage)
    │   └── data.sql                          Les 5 noms lus par le job (exécuté au démarrage)
    └── test/java/com/example/hellobatch
        ├── HelloJobTest.java                 Test d'intégration du job
        └── HelloJobSchedulerTest.java        Test : plusieurs lancements successifs
```

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
| `spring-boot-starter-web` | Serveur web Tomcat embarqué (port 8080), uniquement pour la **console H2** : une petite application web fournie par H2 pour consulter la base dans le navigateur. Activée par `spring.h2.console.enabled=true` dans `application.properties`. |
| `quartz` (2.3.2) | L'ordonnanceur qui lance le job toutes les 5 minutes. Version explicite, car Spring Boot 1.5 ne gère pas Quartz. `c3p0` et `HikariCP-java7` sont exclus : ils ne servent qu'au stockage des planifications en base. |
| `spring-context-support` | L'intégration Spring de Quartz : `SchedulerFactoryBean`, `JobDetailFactoryBean`, `CronTriggerFactoryBean`, `QuartzJobBean` |
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
        SpringApplication.run(HelloBatchApplication.class, args);
    }
}
```

Au démarrage, deux mécanismes travaillent ensemble :

1. `@EnableBatchProcessing` (Spring Batch) crée l'infrastructure : `JobRepository`, `JobLauncher`, `PlatformTransactionManager`, ainsi que les fabriques **`JobBuilderFactory`** et **`StepBuilderFactory`** utilisées pour construire le job ;
2. l'auto-configuration de Spring Boot (`spring-boot-starter-batch`) :
   - crée une `DataSource` vers H2 (puisque H2 est dans le classpath) ;
   - crée les **tables de métadonnées** `BATCH_*` dans cette base (automatique pour une base embarquée).
   - exécute `schema.sql` puis `data.sql` (dans `src/main/resources`) : la table `PERSON` lue par le job.

Par défaut, Spring Boot lancerait aussi le job une fois au démarrage (composant `JobLauncherCommandLineRunner`). C'est désactivé dans `application.properties` (`spring.batch.job.enabled=false`) : c'est Quartz qui lance le job (voir [5.4](#54-quartz--lancer-le-job-toutes-les-5-minutes)).

`SpringApplication.run(...)` démarre le contexte puis rend la main, mais l'application continue de tourner : les threads de Quartz et de Tomcat gardent la JVM en vie. On l'arrête avec Ctrl+C.

> Sans ordonnanceur dans l'application, on écrit plutôt `System.exit(SpringApplication.exit(SpringApplication.run(...)))` : l'application s'arrête dès la fin du job et renvoie un code de sortie (0 = succès) à l'ordonnanceur externe qui l'a lancée (cron, Control-M...). C'était le cas dans les versions précédentes de ce projet.

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
public Step greetStep(JdbcPagingItemReader<String> namesReader) {
    return stepBuilderFactory.get("greetStep")
            .<String, String>chunk(CHUNK_SIZE)   // CHUNK_SIZE = 2
            .reader(namesReader)
            .processor(greetingProcessor())
            .writer(consoleWriter())
            .build();
}
```

- `<String, String>` : type des éléments **lus** (un nom), puis type des éléments **écrits** (une salutation).
- `chunk(CHUNK_SIZE)`, soit 2 : taille du chunk (*commit interval*). Spring Batch lit et transforme 2 éléments, les écrit ensemble, puis valide la transaction. En production on utilise plutôt 100, 500 ou 1000 selon le volume.

Déroulement complet avec 5 noms et des chunks de 2 :

```
Chunk 1 : [transaction] read Alice, read Bob         → process ×2 → write [ALICE, BOB]      [commit]
Chunk 2 : [transaction] read Charlie, read Diane     → process ×2 → write [CHARLIE, DIANE]  [commit]
Chunk 3 : [transaction] read Eve, read → null (fin)  → process ×1 → write [EVE]             [commit]
```

Donc 5 lectures, 5 écritures et 3 commits : ce sont exactement les valeurs vérifiées par le test.

Si le chunk 2 échouait, seule sa transaction serait annulée : le chunk 1 resterait validé, et une relance du job pourrait reprendre à partir de là, car le reader enregistre sa position (voir ci-dessous).

#### Le reader : `namesReader`

Les noms sont lus dans la table `PERSON` de la base H2. Spring Boot crée cette table et la remplit au démarrage, en exécutant automatiquement les fichiers `schema.sql` puis `data.sql` placés dans `src/main/resources`.

```sql
-- schema.sql
CREATE TABLE IF NOT EXISTS PERSON (ID BIGINT PRIMARY KEY, NAME VARCHAR(100) NOT NULL);
-- data.sql
MERGE INTO PERSON (ID, NAME) KEY (ID) VALUES (1, 'Alice');   -- ... jusqu'à 5, 'Eve'
```

`IF NOT EXISTS` et `MERGE` (un « insère ou met à jour » propre à H2) rendent les scripts rejouables : dans les tests, plusieurs contextes Spring partagent la même base en mémoire et exécutent donc les scripts plusieurs fois.

```java
@Bean
@StepScope
public JdbcPagingItemReader<String> namesReader(DataSource dataSource) throws Exception {
    SqlPagingQueryProviderFactoryBean queryProvider = new SqlPagingQueryProviderFactoryBean();
    queryProvider.setDataSource(dataSource);
    queryProvider.setSelectClause("SELECT ID, NAME");
    queryProvider.setFromClause("FROM PERSON");
    queryProvider.setSortKey("ID");

    JdbcPagingItemReader<String> reader = new JdbcPagingItemReader<>();
    reader.setDataSource(dataSource);
    reader.setQueryProvider(queryProvider.getObject());
    reader.setPageSize(CHUNK_SIZE);
    reader.setRowMapper((resultSet, rowNum) -> resultSet.getString("NAME"));
    return reader;
}
```

- Un `ItemReader` renvoie un élément à chaque appel de `read()`, puis `null` quand il a terminé : c'est ce `null` qui met fin au step.
- La requête est décrite en morceaux (`SELECT`, `FROM`, clé de tri). Le *query provider* assemble la requête de chaque page dans le dialecte SQL de la base, détecté à partir de la `DataSource`. Spring Batch 3 n'a pas de *builder* pour ce reader, on règle donc chaque propriété à la main.
- Le `RowMapper` transforme chaque ligne du résultat en élément : ici, juste le nom.
- La `DataSource` est celle créée par Spring Boot, la même base que les tables `BATCH_*`.

**Lecture par pages.** `JdbcPagingItemReader` ne charge pas toute la table. Il exécute une requête par page de `pageSize` lignes, garde la page en mémoire et la distribue ligne par ligne à chaque `read()`. Quand la page est épuisée, il lance la requête suivante. Voici les requêtes réellement exécutées, visibles dans les logs :

```
SELECT TOP 2 ID, NAME FROM PERSON ORDER BY ID ASC                    → Alice, Bob       (chunk 1)
SELECT TOP 2 ID, NAME FROM PERSON WHERE ((ID > ?)) ORDER BY ID ASC   → Charlie, Diane   (chunk 2, ? = 2)
SELECT TOP 2 ID, NAME FROM PERSON WHERE ((ID > ?)) ORDER BY ID ASC   → Eve              (chunk 3, ? = 4)
```

- `TOP 2` est la syntaxe de H2. Sur PostgreSQL ou MySQL, ce serait `LIMIT 2`.
- Les pages suivantes ne font pas `OFFSET n`, qui devient lent sur une grosse table. Elles repartent de la **dernière valeur de la clé de tri** (`WHERE ID > dernier ID lu`), d'où l'obligation d'avoir une clé de tri **unique**.
- La 3e page ne contient qu'une ligne, moins que `pageSize` : le reader sait que c'est la fin et renvoie `null` sans lancer de 4e requête.
- **La taille de page n'est pas la taille du chunk.** C'est un réglage du reader (10 par défaut), indépendant du step. On les prend égales (`CHUNK_SIZE`) pour avoir exactement une requête SQL par chunk. Avec une page de 10 et des chunks de 100, chaque chunk ferait 10 requêtes.
- L'autre reader JDBC, `JdbcCursorItemReader`, fait **une seule requête** pour tout le step et garde le curseur ouvert, en avançant d'une ligne à chaque `read()`. Il est plus simple, mais ne peut pas servir dans un step multi-threadé, et la reprise après échec est plus coûteuse (il réexécute la requête puis saute les lignes déjà lues).

**Pourquoi `@StepScope` ?** Le reader a un état : la page en mémoire et sa position. Sans `@StepScope`, le bean serait un singleton créé une seule fois pour toute la vie de l'application, et au 2e lancement du job il repartirait de la fin de la table : il ne lirait rien. Avec `@StepScope`, Spring crée **un nouveau reader à chaque exécution du step**. `@StepScope` permet aussi d'injecter des paramètres du job dans le bean, par exemple `@Value("#{jobParameters['fichier']}") String fichier`.

**Pourquoi le type de retour est-il `JdbcPagingItemReader` et pas `ItemReader` ?** Ce reader implémente aussi `ItemStream`, et Spring Batch appelle ses méthodes en plus de `read()` :

| Méthode | Quand | Rôle |
|---|---|---|
| `open(executionContext)` | début du step | prépare la lecture, ou relit la position sauvegardée en cas de reprise |
| `update(executionContext)` | avant **chaque commit** | enregistre la position dans `BATCH_STEP_EXECUTION_CONTEXT` |
| `close()` | fin du step | libère les ressources |

Avec `@StepScope`, Spring injecte un proxy qui n'implémente **que le type déclaré** en retour de la méthode `@Bean`. Si ce type était `ItemReader<String>`, le proxy n'implémenterait pas `ItemStream` : `open()` ne serait jamais appelé et la lecture échouerait avec `ReaderNotOpenException`.

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

### 5.4 Quartz : lancer le job toutes les 5 minutes

> Cette section résume le fonctionnement. **[SCHEDULER.md](SCHEDULER.md)** l'explique en détail : ce qui se passe au démarrage, à chaque échéance et à l'arrêt (avec les vrais logs), la syntaxe complète du cron Quartz, les threads, les échéances manquées, le stockage en base et le mode cluster.

Spring Batch **ne sait pas planifier** un job : il sait seulement l'exécuter quand on le lui demande, via le `JobLauncher`. La planification est le rôle d'un autre outil. Ici, c'est **Quartz**, l'ordonnanceur Java le plus répandu dans les applications d'entreprise.

**Deux sortes de « jobs »**, à ne pas confondre :
- le **job Quartz** (`HelloQuartzJob`) dit **quand** faire quelque chose. C'est une simple tâche planifiée, qui ne sait rien de Spring Batch ;
- le **job Spring Batch** (`helloJob`) dit **quoi** faire : steps, chunks, reader, processor, writer, métadonnées en base.

Ici, le job Quartz se contente de lancer le job Spring Batch :

```
Quartz, à 12:05 ─▶ HelloQuartzJob.executeInternal() ─▶ HelloJobScheduler.launch() ─▶ jobLauncher.run(helloJob, {launchTime})
```

#### `QuartzConfig.java` : les trois briques de Quartz

| Brique | Rôle | Ici |
|---|---|---|
| `JobDetail` | **Quoi** exécuter | la classe `HelloQuartzJob` |
| `Trigger` | **Quand** l'exécuter | un cron, `hello.scheduler.cron` |
| `Scheduler` | Le moteur : surveille les triggers et exécute les jobs dans son pool de threads | `SchedulerFactoryBean` |

```java
@Configuration
@ConditionalOnProperty(name = "hello.scheduler.enabled", matchIfMissing = true)
public class QuartzConfig {

    @Bean
    public JobDetailFactoryBean helloJobDetail() {
        JobDetailFactoryBean factory = new JobDetailFactoryBean();
        factory.setJobClass(HelloQuartzJob.class);
        factory.setDurability(true);
        ...
    }

    @Bean
    public CronTriggerFactoryBean helloTrigger(JobDetail jobDetail, @Value("${hello.scheduler.cron}") String cron) {
        CronTriggerFactoryBean factory = new CronTriggerFactoryBean();
        factory.setJobDetail(jobDetail);
        factory.setCronExpression(cron);
        ...
    }

    @Bean
    public SchedulerFactoryBean schedulerFactoryBean(JobDetail jobDetail, Trigger trigger, ApplicationContext ctx) {
        SchedulerFactoryBean factory = new SchedulerFactoryBean();
        factory.setJobDetails(jobDetail);
        factory.setTriggers(trigger);
        factory.setJobFactory(new AutowiringSpringBeanJobFactory(ctx));
        factory.setWaitForJobsToCompleteOnShutdown(true);
        return factory;
    }
}
```

- **Tout est configuré à la main.** Spring Boot 1.5 ne connaît pas Quartz : ni version gérée (d'où `<version>2.3.2</version>` dans le `pom.xml`), ni auto-configuration (le `spring-boot-starter-quartz` n'existe qu'à partir de Spring Boot 2.0). Les classes `*FactoryBean` viennent de `spring-context-support`.
- **`@ConditionalOnProperty`** permet de couper Quartz avec `hello.scheduler.enabled=false`. Les tests s'en servent : ce sont eux qui décident quand lancer le job, pas l'horloge.
- **`setWaitForJobsToCompleteOnShutdown(true)`** : à l'arrêt (Ctrl+C), Quartz attend la fin d'un job en cours au lieu de le couper au milieu.
- **Planifications en mémoire (`RAMJobStore`).** Aucune `DataSource` n'est donnée au `SchedulerFactoryBean` : Quartz garde ses planifications en mémoire et les recrée au démarrage à partir de cette configuration. On le voit dans les logs : `Using job-store 'org.quartz.simpl.RAMJobStore' - which does not support persistence. and is not clustered.` Avec une `DataSource`, il les stockerait dans des tables `QRTZ_*`. C'est ce qui permet le **mode cluster** (une seule instance de l'application exécute chaque échéance) et le **rattrapage des échéances manquées** pendant un arrêt. C'est inutile ici : une seule instance, et une base H2 elle-même en mémoire. C'est pour ça que les dépendances `c3p0` et `HikariCP-java7`, qui ne servent qu'à ce stockage, sont exclues dans le `pom.xml`.

#### Le cron de Quartz

L'expression vient de `application.properties` : `hello.scheduler.cron=0 */5 * * * ?`, soit « à la seconde 0 de chaque minute multiple de 5 ». Comme le cron de Spring, il a **6 champs** : `seconde minute heure jour-du-mois mois jour-de-la-semaine`. Mais Quartz impose que **l'un des deux champs « jour » vaille `?`** (« peu importe »), car il refuse qu'on précise à la fois un jour du mois et un jour de la semaine. Avec `0 */5 * * * *` (valable pour `@Scheduled`), l'application ne démarre pas :

```
java.text.ParseException: Support for specifying both a day-of-week AND a day-of-month parameter is not implemented.
```

#### `HelloQuartzJob.java` : le job Quartz

```java
@DisallowConcurrentExecution
public class HelloQuartzJob extends QuartzJobBean {

    @Autowired
    private HelloJobScheduler scheduler;

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        try {
            scheduler.launch();
        } catch (Exception e) {
            throw new JobExecutionException("Échec du lancement de helloJob", e);
        }
    }
}
```

- **Ce n'est pas un bean Spring.** Quartz crée lui-même **une nouvelle instance** de cette classe à chaque échéance, en dehors de Spring. Pour que le champ `@Autowired` soit rempli, `QuartzConfig` donne à Quartz une *JobFactory* (`AutowiringSpringBeanJobFactory`) qui crée l'instance puis demande à Spring d'y injecter les dépendances. Sans elle, `scheduler` serait `null` et on aurait une `NullPointerException` à la première échéance. (En Spring 4.3, la `SpringBeanJobFactory` de base ne le fait pas d'elle-même ; les versions récentes le font.)
- **`@DisallowConcurrentExecution`** : Quartz exécute ses jobs dans un **pool de 10 threads** (`SimpleThreadPool`). Si un lancement durait plus de 5 minutes, le suivant démarrerait en parallèle sur un autre thread. Cette annotation l'interdit : l'échéance suivante attend la fin de la précédente.
- Quartz n'accepte que des `JobExecutionException` : on « emballe » l'erreur. Quartz la journalise, et le trigger continue de se déclencher aux échéances suivantes.

#### `HelloJobScheduler.java` : le lancement du job Spring Batch

```java
@Component
public class HelloJobScheduler {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private Job helloJob;

    public JobExecution launch() throws Exception {
        JobParameters parameters = new JobParametersBuilder()
                .addLong("launchTime", System.currentTimeMillis())
                .toJobParameters();
        return jobLauncher.run(helloJob, parameters);
    }
}
```

- **Le paramètre `launchTime` est indispensable.** Une JobInstance est identifiée par le nom du job et ses paramètres (voir [section 7](#7-le-jobrepository-et-les-tables-de-métadonnées)). Avec des paramètres identiques à chaque fois, le 2e lancement serait vu comme la même JobInstance, déjà terminée, et Spring Batch le refuserait (`JobInstanceAlreadyCompleteException`). L'heure du lancement rend chaque JobInstance unique.
- **`@StepScope` sur le reader devient indispensable** : le job tourne plusieurs fois dans la même application. Sans lui, le 2e lancement ne lirait rien (voir [le reader](#le-reader--namesreader)).
- Cette classe ne dépend pas de Quartz : le test l'appelle directement, sans attendre l'horloge.

#### `@Scheduled` ou Quartz ?

Une version précédente de ce projet utilisait le scheduler intégré à Spring (`@EnableScheduling` + `@Scheduled(cron = "...")`), plus simple :

| | `@Scheduled` (Spring) | Quartz |
|---|---|---|
| Dépendances | Aucune, inclus dans `spring-context` | `quartz` + `spring-context-support` |
| Configuration | Une annotation | `JobDetail`, `Trigger`, `SchedulerFactoryBean`, *JobFactory* |
| Threads | 1 par défaut : pas de chevauchement | Pool de 10 : `@DisallowConcurrentExecution` nécessaire |
| Cron | 6 champs, `*` accepté partout | 6 champs (+ année optionnelle), `?` obligatoire pour un des champs « jour » |
| Planifications | En mémoire | En mémoire (`RAMJobStore`) **ou en base** (`QRTZ_*`) |
| Plusieurs instances de l'application | Chacune lance le job : exécutions **en double** | Mode cluster (avec stockage en base) : une seule exécute |
| Échéance manquée pendant un arrêt | Perdue | Rattrapage configurable (*misfire*), avec stockage en base |
| Modifier les planifications à chaud | Non | Oui, par l'API `Scheduler` |

`@Scheduled` suffit pour une application seule avec des planifications fixes. Quartz devient utile dès qu'il y a plusieurs instances, un besoin de ne perdre aucune échéance, ou des planifications gérées dynamiquement.

---

## 6. Ce qui se passe à l'exécution

```
mvn spring-boot:run
  │
  ├─ Spring Boot démarre
  │    ├─ crée la DataSource H2 (en mémoire)
  │    ├─ crée les tables BATCH_*
  │    ├─ exécute schema.sql et data.sql : table PERSON avec 5 noms
  │    ├─ @EnableBatchProcessing : JobRepository, JobLauncher, TransactionManager,
  │    │                           JobBuilderFactory, StepBuilderFactory
  │    ├─ crée les beans de HelloJobConfig (helloStep, greetStep, helloJob)
  │    ├─ QuartzConfig : démarre Quartz (RAMJobStore, pool de 10 threads)
  │    └─ démarre Tomcat sur le port 8080 (console H2 : /h2-console)
  │
  ├─ main() se termine, mais la JVM reste en vie (threads de Quartz et de Tomcat)
  │
  ├─ à 12:05, 12:10, 12:15... Quartz exécute HelloQuartzJob → HelloJobScheduler.launch()
  │    └─ jobLauncher.run(helloJob, {launchTime=...})
  │         ├─ nouvelle JobInstance + JobExecution créées en base  (statut STARTED)
  │         ├─ helloStep : exécute la tasklet                     (StepExecution COMPLETED)
  │         ├─ greetStep : 3 chunks lus / transformés / écrits    (StepExecution COMPLETED)
  │         └─ JobExecution mise à jour                           (statut COMPLETED)
  │
  └─ Ctrl+C → fermeture du contexte Spring → la JVM s'arrête
```

Comme H2 est **en mémoire**, la base disparaît à l'arrêt de l'application. En revanche, tant que l'application tourne, elle accumule une JobInstance par lancement.

---

## 7. Le JobRepository et les tables de métadonnées

Le `JobRepository` enregistre tout ce que fait Spring Batch dans **6 tables**, créées au démarrage par Spring Boot (script `schema-h2.sql` de Spring Batch). Elles suivent la hiérarchie des concepts :

```
BATCH_JOB_INSTANCE                    "le job helloJob avec launchTime=…"   (1 ligne par lancement unique)
 └─ BATCH_JOB_EXECUTION               une tentative d'exécution             (1 ligne par tentative)
     ├─ BATCH_JOB_EXECUTION_PARAMS    ses paramètres                        (1 ligne par paramètre)
     ├─ BATCH_JOB_EXECUTION_CONTEXT   données sauvegardées du job           (1 ligne)
     └─ BATCH_STEP_EXECUTION          une exécution de step                 (1 ligne par step)
         └─ BATCH_STEP_EXECUTION_CONTEXT  données sauvegardées du step      (1 ligne)
```

Il y a aussi 3 **séquences** (`BATCH_JOB_SEQ`, `BATCH_JOB_EXECUTION_SEQ`, `BATCH_STEP_EXECUTION_SEQ`), qui ne servent qu'à générer les identifiants.

Ces tables permettent à Spring Batch de :
- **refuser de relancer** un job déjà terminé avec succès avec les mêmes paramètres (`JobInstanceAlreadyCompleteException`) ;
- **reprendre** un job en échec au step (et à la position) où il s'était arrêté ;
- garder un **historique** consultable.

### À quoi sert chaque table

Les exemples ci-dessous sont les valeurs réellement trouvées dans H2 après deux lancements de `helloJob`.

**`BATCH_JOB_INSTANCE`** : l'identité d'un traitement, « le job X avec tels paramètres ».
- Colonnes principales : `JOB_NAME` et `JOB_KEY`, une empreinte (hash MD5) des paramètres identifiants. Le couple (`JOB_NAME`, `JOB_KEY`) est **unique**.
- C'est grâce à elle que Spring Batch refuse de relancer un job déjà réussi avec les mêmes paramètres.
- Exemple : `helloJob / 237a1c…`, puis `helloJob / 216680…`. Les clés sont différentes parce que `launchTime` change.

**`BATCH_JOB_EXECUTION`** : une **tentative** d'exécution de cette instance.
- Colonnes principales : `STATUS` (`STARTING` → `STARTED` → `COMPLETED` / `FAILED`), `EXIT_CODE`, `START_TIME`, `END_TIME`, `EXIT_MESSAGE` (la stack trace en cas d'échec).
- Si un job échoue puis réussit à la relance, on a 1 `JOB_INSTANCE` et 2 `JOB_EXECUTION`.

**`BATCH_JOB_EXECUTION_PARAMS`** : les paramètres de lancement, une ligne par paramètre.
- Exemple : `KEY_NAME=launchTime, TYPE_CD=LONG, LONG_VAL=1790766945994, IDENTIFYING=Y`.
- `IDENTIFYING=Y` : ce paramètre entre dans le calcul de `JOB_KEY`, donc dans l'identité de la JobInstance.

**`BATCH_STEP_EXECUTION`** : l'exécution d'**un step**, avec tous ses **compteurs**. C'est la table à consulter pour savoir ce que le batch a traité.

| `STEP_NAME` | `STATUS` | `COMMIT_COUNT` | `READ_COUNT` | `WRITE_COUNT` | `FILTER_COUNT` | `ROLLBACK_COUNT` |
|---|---|---|---|---|---|---|
| `helloStep` | `COMPLETED` | 1 | 0 (une tasklet ne lit rien) | 0 | 0 | 0 |
| `greetStep` | `COMPLETED` | 3 | 5 | 5 | 0 | 0 |

**`BATCH_JOB_EXECUTION_CONTEXT`** et **`BATCH_STEP_EXECUTION_CONTEXT`** : la « mémoire » du job et de chaque step, sous forme de clés-valeurs converties en JSON (par la bibliothèque XStream en Spring Batch 3).
- Leur rôle principal est la **reprise après échec**. Un `FlatFileItemReader` y écrit par exemple `FlatFileItemReader.read.count=4200` après chaque chunk. Si le job plante, la relance repart de la ligne 4 201.
- On peut aussi y déposer ses propres données pour les passer d'un step à l'autre.
- Dans ce projet, elles contiennent :
  - contexte du job : vide (`{"map":[""]}`) ;
  - contexte des steps : des informations techniques ajoutées par Spring Batch, `batch.stepType=TaskletStep` et `batch.taskletType=ChunkOrientedTasklet` pour `greetStep`. On voit au passage qu'un step « chunk » est en réalité un `TaskletStep` ;
  - pour `greetStep`, la **position du reader**, enregistrée par `JdbcPagingItemReader` avant chaque commit : `JdbcPagingItemReader.start.after = {ID=5}` (la dernière clé lue, pour reprendre avec `WHERE ID > 5`) et `JdbcPagingItemReader.read.count = 6` (5 noms + le dernier `read()` qui a renvoyé `null`).

### À quel moment elles sont remplies

Pour un lancement du job :

| Moment | Ce qui se passe en base |
|---|---|
| **`jobLauncher.run(...)`**, avant le démarrage | `SELECT` sur `JOB_INSTANCE` : cette instance existe-t-elle déjà ? Si non : **`INSERT`** dans `JOB_INSTANCE`, `JOB_EXECUTION` (statut `STARTING`), `JOB_EXECUTION_PARAMS` et `JOB_EXECUTION_CONTEXT` |
| **Démarrage du job** | `UPDATE JOB_EXECUTION` → `STARTED`, `START_TIME` renseigné |
| **Démarrage de chaque step** | **`INSERT`** dans `STEP_EXECUTION` (compteurs à 0) et `STEP_EXECUTION_CONTEXT`, puis `UPDATE` → `STARTED` |
| **Après chaque chunk** (ou chaque appel de la tasklet) | `UPDATE STEP_EXECUTION_CONTEXT` et `UPDATE STEP_EXECUTION` (compteurs), **dans la même transaction que l'écriture des données** |
| **Fin de chaque step** | `UPDATE STEP_EXECUTION` → `COMPLETED`, `END_TIME` |
| **Fin du job** | `UPDATE JOB_EXECUTION_CONTEXT`, puis `UPDATE JOB_EXECUTION` → `COMPLETED`, `END_TIME`, `EXIT_CODE` |

Le point important est la mise à jour **après chaque chunk, dans la même transaction**. Si le chunk 3 plante, sa transaction est annulée, mais les chunks 1 et 2 sont validés, et les compteurs et le contexte en base le reflètent exactement (4 éléments lus et écrits). La base ne peut jamais indiquer « 6 écrits » alors que seulement 4 le sont réellement : c'est ce qui rend la reprise fiable.

Les tables ne font que **grossir** : Spring Batch ajoute des lignes et n'en supprime jamais. Avec Quartz, chaque lancement ajoute 1 instance, 1 exécution, 1 paramètre, 1 contexte de job, 2 exécutions de step et 2 contextes de step. Avec H2 en mémoire, tout disparaît à l'arrêt de l'application. Sur une vraie base, il faut prévoir une **purge régulière**, que Spring Batch ne fait pas lui-même.

### Consulter la base avec la console H2

La console H2 est une petite application web fournie par H2. Spring Boot la publie sur **http://localhost:8080/h2-console** grâce à `spring.h2.console.enabled=true` et à `spring-boot-starter-web`, qui apporte le serveur web. On s'y connecte avec la JDBC URL **`jdbc:h2:mem:testdb`**, l'utilisateur `sa` et un mot de passe vide.

Une base H2 « en mémoire » n'existe qu'à l'intérieur de la JVM qui l'a créée : un outil SQL externe ne pourrait pas s'y connecter. La console fonctionne parce qu'elle tourne **dans la même JVM** que l'application. Elle ouvre la base par son nom (`testdb`, le nom par défaut choisi par Spring Boot 1.5) et retrouve donc exactement les tables utilisées par le job.

Requêtes utiles :

```sql
-- Les données lues par le job
SELECT * FROM PERSON;

-- Une ligne par lancement (par échéance Quartz)
SELECT JOB_INSTANCE_ID, JOB_NAME, JOB_KEY FROM BATCH_JOB_INSTANCE;

-- Statut et durée de chaque exécution
SELECT JOB_EXECUTION_ID, STATUS, EXIT_CODE, START_TIME, END_TIME FROM BATCH_JOB_EXECUTION;

-- Le paramètre launchTime de chaque exécution
SELECT JOB_EXECUTION_ID, KEY_NAME, LONG_VAL FROM BATCH_JOB_EXECUTION_PARAMS;

-- Les compteurs de chaque step : 5 lus, 5 écrits, 3 commits pour greetStep
SELECT STEP_EXECUTION_ID, JOB_EXECUTION_ID, STEP_NAME, STATUS,
       READ_COUNT, FILTER_COUNT, WRITE_COUNT, COMMIT_COUNT, ROLLBACK_COUNT
FROM BATCH_STEP_EXECUTION ORDER BY STEP_EXECUTION_ID;

-- La position enregistrée par le reader (JdbcPagingItemReader.start.after, read.count)
SELECT STEP_EXECUTION_ID, SHORT_CONTEXT FROM BATCH_STEP_EXECUTION_CONTEXT;
```

Astuce : relance une requête après une échéance de Quartz (toutes les 5 minutes, ou toutes les 10 secondes avec `--hello.scheduler.cron=*/10 * * * * ?`) pour voir les nouvelles lignes s'ajouter.

### Voir les requêtes dans les logs

Spring Batch accède à ces tables via le `JdbcTemplate` de Spring. Dans `application.properties`, cette ligne affiche chaque requête dans la console :

```properties
logging.level.org.springframework.jdbc.core.JdbcTemplate=DEBUG
```

Voici la séquence d'**une** exécution de `helloJob` (logs simplifiés, colonnes abrégées) :

```
-- Le JobLauncher vérifie qu'aucune JobInstance n'existe déjà pour ces paramètres...
SELECT JOB_INSTANCE_ID, JOB_NAME from BATCH_JOB_INSTANCE where JOB_NAME = ? and JOB_KEY = ?
-- ... puis crée la JobInstance, la JobExecution, ses paramètres (launchTime) et son contexte
INSERT into BATCH_JOB_INSTANCE(...)
INSERT into BATCH_JOB_EXECUTION(...)
INSERT into BATCH_JOB_EXECUTION_PARAMS(...)
INSERT INTO BATCH_JOB_EXECUTION_CONTEXT (...)
Job: [SimpleJob: [name=helloJob]] launched with the following parameters: [{launchTime=...}]
UPDATE BATCH_JOB_EXECUTION set ... STATUS = ? ...                 -- STARTED

-- Chaque step : création de la StepExecution
INSERT into BATCH_STEP_EXECUTION(...)
INSERT INTO BATCH_STEP_EXECUTION_CONTEXT (...)
Executing step: [greetStep]
UPDATE BATCH_STEP_EXECUTION set ... STATUS = ? ...                -- STARTED
--- writing chunk of 2 item(s)
UPDATE BATCH_STEP_EXECUTION_CONTEXT SET ...                       -- après CHAQUE chunk,
UPDATE BATCH_STEP_EXECUTION set ... COMMIT_COUNT = ?, READ_COUNT = ?, WRITE_COUNT = ? ...
--- writing chunk of 2 item(s)                                   -- dans la transaction du chunk
UPDATE BATCH_STEP_EXECUTION_CONTEXT SET ...
UPDATE BATCH_STEP_EXECUTION set ...
--- writing chunk of 1 item(s)
...
UPDATE BATCH_STEP_EXECUTION set ... STATUS = ? ...                -- COMPLETED

-- Fin du job
UPDATE BATCH_JOB_EXECUTION_CONTEXT SET ...
UPDATE BATCH_JOB_EXECUTION set ... STATUS = ? ...                 -- COMPLETED
```

Le point clé : **après chaque chunk**, Spring Batch enregistre les compteurs et le contexte du step, dans la même transaction que l'écriture des données. Si le job plante au chunk 3, la base indique exactement ce qui a été validé (2 chunks, 4 éléments). C'est ce qui permet la reprise.

Les requêtes affichent des `?` à la place des valeurs. Les valeurs (`STARTED`, `COMPLETED`, compteurs...) sont affichées juste après chaque requête, grâce à cette autre ligne de `application.properties` :

```properties
logging.level.org.springframework.jdbc.core.StatementCreatorUtils=TRACE
```

C'est très bavard (une ligne par paramètre, une vingtaine par `UPDATE`) : on peut la commenter avec `#` pour n'afficher que les requêtes.

### Déduire la taille des chunks

La taille des chunks **n'est enregistrée dans aucune table** : Spring Batch stocke ce qui s'est passé (les compteurs), pas la configuration du step. On peut la retrouver de trois façons, de la plus fiable à la moins fiable.

**1. Dans le code : la seule source sûre.**

```java
.<String, String>chunk(2)                                     // configuration Java
```

```xml
<batch:chunk reader="..." writer="..." commit-interval="2"/>  <!-- configuration XML -->
```

La valeur vient parfois d'une propriété (`${batch.chunk.size}`) ou d'un paramètre du job (`#{jobParameters['chunkSize']}`). Il faut alors aller voir le fichier de propriétés ou les paramètres de lancement.

**2. Dans les logs : chunk par chunk.** Juste avant chaque commit, Spring Batch écrit en `DEBUG` l'état du step :

```properties
logging.level.org.springframework.batch.core.step.tasklet.TaskletStep=DEBUG
```

```
Saving step execution before commit: ... name=greetStep, readCount=2, writeCount=2, commitCount=1 ...
Saving step execution before commit: ... name=greetStep, readCount=4, writeCount=4, commitCount=2 ...
Saving step execution before commit: ... name=greetStep, readCount=5, writeCount=5, commitCount=3 ...
```

La différence de `readCount` entre deux lignes donne la taille de chaque chunk : 2, 2, puis 1. La taille configurée est celle des chunks « pleins », donc **2**.

**3. Dans `BATCH_STEP_EXECUTION` : une estimation.**

Il faut d'abord savoir que `COMMIT_COUNT` n'est pas tout à fait le nombre de chunks. Pour savoir qu'il a fini, Spring Batch doit recevoir `null` du reader. Si ce `null` arrive au début d'un nouveau tour, ce tour vide est **quand même validé** par une transaction. Avec des chunks de 2 :

| Éléments lus | Chunks réels | `COMMIT_COUNT` |
|---|---|---|
| 5 | [2] [2] [1] | 3 |
| 4 | [2] [2] + tour vide | 3 (et non 2) |
| 0 | tour vide | 1 (et non 0) |

Pour une exécution sans erreur, on a donc `COMMIT_COUNT − 1 = partie entière de (READ_COUNT / taille)`. On en déduit un **intervalle** :

```
READ_COUNT / COMMIT_COUNT  <  taille  ≤  READ_COUNT / (COMMIT_COUNT − 1)
```

| `READ_COUNT` | `COMMIT_COUNT` | Intervalle | Taille |
|---|---|---|---|
| 5 | 3 | ]1,67 ; 2,5] | **2** exactement |
| 10 000 | 11 | ]909 ; 1 000] | entre 910 et 1 000, très probablement **1 000** |
| 1 500 | 2 | ]750 ; 1 500] | ambigu : 1 000 ? 1 500 ? |

Avec beaucoup de chunks, l'intervalle est étroit. Avec peu de chunks, il est flou. En pratique, on retient la valeur « ronde » (100, 500, 1 000…) qui tombe dans l'intervalle.

```sql
SELECT STEP_NAME, READ_COUNT, COMMIT_COUNT,
       CAST(READ_COUNT AS DOUBLE) / COMMIT_COUNT       AS TAILLE_MIN_EXCLUE,
       CAST(READ_COUNT AS DOUBLE) / (COMMIT_COUNT - 1) AS TAILLE_MAX
FROM BATCH_STEP_EXECUTION
WHERE COMMIT_COUNT > 1 AND ROLLBACK_COUNT = 0
  AND READ_SKIP_COUNT + PROCESS_SKIP_COUNT + WRITE_SKIP_COUNT = 0;
```

Ce calcul est faux ou sans objet dans ces cas :
- **Il y a eu des erreurs** (`ROLLBACK_COUNT > 0` ou des skips). En mode `faultTolerant`, quand l'écriture d'un chunk échoue, Spring Batch le rejoue élément par élément pour isoler le fautif : un commit par élément. D'où le filtre dans la requête.
- **La taille n'est pas fixe** : au lieu de `.chunk(n)`, on peut fermer le chunk avec une `CompletionPolicy` (selon une durée, par exemple).
- **Le step est une tasklet** (`helloStep`) : pas de chunk, et `COMMIT_COUNT` compte les appels de la tasklet.

`FILTER_COUNT` n'intervient pas : un élément filtré par le processor a quand même été lu et compte dans le remplissage du chunk. Un chunk de 2 peut donc n'écrire qu'un seul élément, voire aucun.

### Relancer un job

Relancer `helloJob` avec les mêmes paramètres échouerait donc la 2e fois. C'est pour ça que `HelloJobScheduler.launch()` passe un paramètre `launchTime` qui change à chaque lancement. Il existe aussi le `RunIdIncrementer` (`.incrementer(new RunIdIncrementer())`), mais il n'est utilisé que par certains lanceurs (`JobLauncherCommandLineRunner`, `JobOperator.startNextInstance`), pas par un appel direct à `jobLauncher.run(...)`.

---

## 8. Le test

`src/test/java/com/example/hellobatch/HelloJobTest.java`

```java
@RunWith(SpringRunner.class)
@SpringBootTest(properties = {"spring.batch.job.enabled=false", "hello.scheduler.enabled=false"})
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
- C'est le test qui doit décider quand lancer le job, pas le démarrage ni l'horloge :
  - `spring.batch.job.enabled=false` **désactive le lancement automatique** au démarrage (c'est déjà le cas dans `application.properties`, mais le test le rend explicite) ;
  - `hello.scheduler.enabled=false` **désactive Quartz** (voir [5.4](#54-quartz--lancer-le-job-toutes-les-5-minutes)).
- Le bean `JobLauncherTestUtils` est **déclaré à la main** dans une `@TestConfiguration` (l'annotation `@SpringBatchTest`, qui le fait automatiquement, n'existe qu'à partir de Spring Batch 4.1). Ses setters sont `@Autowired` : il reçoit l'unique bean `Job`, le `JobLauncher` et le `JobRepository`.
- `launchJob()` lance le job **de façon synchrone** avec des paramètres uniques (un nombre aléatoire). Chaque appel crée donc une nouvelle JobInstance et le test peut être rejoué sans erreur « job déjà terminé ».
- On vérifie ensuite le résultat grâce aux métadonnées : le statut du job et les compteurs du `StepExecution` de `greetStep`.
- `JobLauncherTestUtils` permet aussi de tester **un seul step** : `launchStep("greetStep")`.

`src/test/java/com/example/hellobatch/HelloJobSchedulerTest.java` vérifie ce que fait Quartz toutes les 5 minutes, sans attendre : il appelle deux fois de suite `HelloJobScheduler.launch()` et vérifie que :
- les deux lancements se terminent en `COMPLETED`, avec deux JobInstances différentes (grâce au paramètre `launchTime`) ;
- le 2e lancement lit bien 5 noms (grâce à `@StepScope` sur le reader).

---

## 9. Pièges classiques

| Piège | Symptôme | Solution |
|---|---|---|
| Oublier `@EnableBatchProcessing` (Spring Boot 1.x) | Aucun bean `JobBuilderFactory` / `StepBuilderFactory` au démarrage | Ajouter l'annotation sur une classe `@Configuration` |
| Reader avec état déclaré en singleton | Le 2e lancement du job ne lit rien (0 élément) | `@StepScope` sur le bean reader (ou `scope="step"` en XML) |
| Méthode `@Bean @StepScope` qui déclare `ItemReader<T>` en retour | `ReaderNotOpenException` : le proxy n'implémente pas `ItemStream`, donc `open()` n'est jamais appelé | Déclarer le type concret (`JdbcPagingItemReader<T>`, `FlatFileItemReader<T>`...) ou `ItemStreamReader<T>` |
| Relancer un job terminé avec les mêmes paramètres | `JobInstanceAlreadyCompleteException` (par exemple à la 2e échéance de Quartz) | Paramètres différents à chaque lancement (`launchTime`), ou `RunIdIncrementer` |
| Cron au format Spring (`0 */5 * * * *`) avec Quartz | Démarrage impossible : `ParseException: Support for specifying both a day-of-week AND a day-of-month parameter is not implemented` | Mettre `?` dans un des champs « jour » : `0 */5 * * * ?` |
| Job Quartz sans *JobFactory* qui injecte les beans | `NullPointerException` à la première échéance : les champs `@Autowired` du job Quartz sont `null` (Quartz crée l'instance hors de Spring) | `setJobFactory(...)` avec une `SpringBeanJobFactory` qui appelle `autowireBean` (voir `QuartzConfig`) |
| Job Quartz sans `@DisallowConcurrentExecution` | Si un lancement dure plus longtemps que l'intervalle, le suivant démarre en parallèle (pool de 10 threads) | Annoter la classe du job Quartz |
| Plusieurs `Job` dans le contexte | Spring Boot les lance **tous** au démarrage | Préciser `spring.batch.job.names=monJob` (avec un « s » en Spring Boot 1.x) |
| Pas de base de données | Erreur au démarrage : aucune `DataSource` | Ajouter une base (H2 pour apprendre) |
| Console H2 : mauvaise JDBC URL | `Database "..." not found, and IFEXISTS=true, so we cant auto-create it` : la console vise une autre base, et H2 lui interdit d'en créer une | JDBC URL `jdbc:h2:mem:testdb` (et non la valeur préremplie `jdbc:h2:~/test`), application démarrée |
| JDK 11+ sans `javax.annotation-api` | `Table "BATCH_JOB_INSTANCE" not found` | Ajouter la dépendance (voir [5.1](#51-pomxml)) |
| JDK 16+ sans `--add-opens` | `InaccessibleObjectException ... does not "opens java.lang"` | Options `--add-opens` (voir [5.1](#51-pomxml)) |
| Code Java 9+ (`List.of`, `var`, `record`...) | Erreur de compilation avec `java.version` 1.8 | Équivalents Java 8 : `Arrays.asList`, types explicites, classes classiques |

---

## 10. Pour aller plus loin

Idées pour faire évoluer cet exemple, dans un ordre progressif :

1. **Lire un fichier CSV** avec `FlatFileItemReader` et le transformer en objets (une classe `Person` avec `firstName` et `lastName`). En Spring Batch 3, il n'y a pas de `FlatFileItemReaderBuilder` : on assemble soi-même un `FlatFileItemReader`, un `DefaultLineMapper`, un `DelimitedLineTokenizer` et un `BeanWrapperFieldSetMapper`.
2. **Écrire en base** avec `JdbcBatchItemWriter` (par exemple les salutations dans une table `GREETING`), puis observer le résultat dans la console H2.
3. **Passer des paramètres au job** : en ajouter dans `HelloJobScheduler.launch()` (par exemple `.addString("fichier", "data.csv")`) et les lire avec `@Value("#{jobParameters['fichier']}")` dans un bean `@StepScope`. (Sans scheduler, avec le lancement au démarrage par Spring Boot, ils viendraient de la ligne de commande : `java -jar hello-batch.jar fichier=data.csv`.)
4. **Gérer les erreurs** : `.faultTolerant().skip(FlatFileParseException.class).skipLimit(10)` pour ignorer les lignes invalides, `.retry(...)` pour réessayer.
5. **Tester la reprise** : faire échouer le job au milieu, puis le relancer avec les mêmes paramètres et constater qu'il reprend où il s'était arrêté.
6. **Ajouter des listeners** (`JobExecutionListener`, `StepExecutionListener`, `ChunkListener`) pour tracer le début et la fin des traitements. Un reader peut lui-même implémenter `StepExecutionListener` : sa méthode `beforeStep(StepExecution)` est appelée avant la première lecture.
7. **Écrire le même job en XML** (`<batch:job>`, `<batch:step>`, `<batch:tasklet>`, `<batch:chunk reader="..." commit-interval="2"/>`), le style dominant dans les projets Spring Batch 2 et 3, et le charger avec `@ImportResource`.
8. **Quartz avec stockage en base** : donner une `DataSource` au `SchedulerFactoryBean` (tables `QRTZ_*`, scripts fournis dans le jar de Quartz), puis lancer deux instances de l'application en mode cluster et constater qu'une seule exécute chaque échéance.
9. **Flux conditionnels** : `.on("FAILED").to(...)`, ou un `JobExecutionDecider`.
10. **Paralléliser** : step multi-threadé (`.taskExecutor(...)`), partitionnement.

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
