package com.example.hellobatch;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Définition du job "helloJob".
 *
 * <p>Vocabulaire :
 * <ul>
 *   <li><b>Job</b> : un traitement batch complet, composé d'une suite de Steps.</li>
 *   <li><b>Step</b> : une étape du job. Deux styles existent :
 *     <ul>
 *       <li><b>Tasklet</b> : une seule action (appeler un service, supprimer un fichier...) ;</li>
 *       <li><b>Chunk</b> : lire / transformer / écrire une grande quantité d'éléments,
 *           par paquets ("chunks"), chaque paquet dans sa propre transaction.</li>
 *     </ul>
 *   </li>
 * </ul>
 *
 * <pre>
 *   helloJob
 *    ├── helloStep   (Tasklet : affiche "Hello World")
 *    └── greetStep   (Chunk   : noms -> "Hello, NOM!" -> console, par paquets de 2)
 * </pre>
 *
 * <p>Tous les éléments (Job, Steps, Reader) sont déclarés comme des beans Spring
 * ({@code @Bean}) : Spring les crée et les injecte les uns dans les autres.
 */
@Configuration
public class HelloJobConfig {

    /**
     * Taille des chunks de greetStep, réutilisée comme taille de page du reader JDBC :
     * chaque chunk correspond ainsi à exactement une requête SQL.
     */
    private static final int CHUNK_SIZE = 2;

    /**
     * Fabriques fournies par {@code @EnableBatchProcessing} (voir {@link HelloBatchApplication}).
     *
     * <p>C'est la façon de construire jobs et steps en Spring Batch 3 et 4 : la fabrique
     * connaît déjà le {@code JobRepository} (et, pour les steps, le transaction manager),
     * on n'a donc pas à les passer nous-mêmes. Spring Batch 5 a supprimé ces fabriques
     * au profit de {@code new JobBuilder(nom, jobRepository)}.
     */
    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    // =====================================================================
    // Step 1 : Tasklet
    // =====================================================================

    /**
     * Step de type Tasklet : exécute une seule action.
     *
     * <p>Le {@code JobRepository} (qui enregistre en base l'état du step : démarré, terminé,
     * en échec...) et le transaction manager (l'exécution de la tasklet est entourée d'une
     * transaction) sont fournis implicitement par {@code stepBuilderFactory}.
     */
    @Bean
    public Step helloStep() {
        // "helloStep" est le nom du step : c'est ce nom qui apparaît dans les logs
        // et dans la table BATCH_STEP_EXECUTION.
        return stepBuilderFactory.get("helloStep")
                // Une Tasklet est une interface fonctionnelle : on peut l'écrire en lambda (Java 8).
                //  - contribution : permet de mettre à jour les compteurs du step (lectures, écritures...)
                //  - chunkContext : donne accès au contexte d'exécution (paramètres du job, etc.)
                .tasklet((contribution, chunkContext) -> {
                    System.out.println(">>> Hello World from Spring Batch!");
                    // FINISHED    => la tasklet a terminé, on passe à la suite.
                    // CONTINUABLE => Spring Batch rappellerait la tasklet (utile pour boucler).
                    return RepeatStatus.FINISHED;
                })
                .build();
    }

    // =====================================================================
    // Step 2 : Chunk (Reader -> Processor -> Writer)
    // =====================================================================

    /**
     * Step orienté "chunk" : c'est le cœur de Spring Batch.
     *
     * <p>Fonctionnement, pour un chunk de taille 2 :
     * <pre>
     *   début transaction
     *     read()    -> "Alice"            \
     *     read()    -> "Bob"              /  on lit jusqu'à avoir 2 éléments
     *     process("Alice"), process("Bob")   on transforme chaque élément
     *     write(["Hello, ALICE!", "Hello, BOB!"])  on écrit le paquet d'un coup
     *   commit
     *   ... on recommence jusqu'à ce que read() renvoie null.
     * </pre>
     *
     * <p>Si une erreur survient pendant un chunk, seule la transaction de ce chunk
     * est annulée : les chunks précédents restent validés.
     *
     * @param namesReader le reader déclaré plus bas ; Spring l'injecte ici
     */
    @Bean
    public Step greetStep(JdbcPagingItemReader<String> namesReader) {
        return stepBuilderFactory.get("greetStep")
                // <String, String> : type lu par le reader, type écrit par le writer.
                // CHUNK_SIZE (2) : taille du chunk = nombre d'éléments par transaction (commit interval).
                //     En production on prend plutôt 100, 500, 1000... selon le volume.
                .<String, String>chunk(CHUNK_SIZE)
                .reader(namesReader)
                .processor(greetingProcessor())
                .writer(consoleWriter())
                .build();
    }

    /**
     * ItemReader : fournit les éléments un par un, ici en lisant la table PERSON par JDBC
     * (table créée et remplie au démarrage par schema.sql et data.sql).
     *
     * <p>Contrat : chaque appel à {@code read()} renvoie l'élément suivant,
     * et renvoie {@code null} quand il n'y a plus rien à lire (fin du step).
     *
     * <p><b>Lecture par pages.</b> {@code JdbcPagingItemReader} ne charge pas toute la table :
     * il exécute une requête par page de {@code pageSize} lignes, garde la page en mémoire et
     * la distribue ligne par ligne à chaque {@code read()}. Quand la page est épuisée, il lance
     * la requête suivante. Avec 5 personnes et des pages de 2 :
     * <pre>
     *   page 1 : SELECT TOP 2 ID, NAME FROM PERSON ORDER BY ID ASC                   -> Alice, Bob
     *   page 2 : SELECT TOP 2 ID, NAME FROM PERSON WHERE ((ID > 2)) ORDER BY ID ASC  -> Charlie, Diane
     *   page 3 : SELECT TOP 2 ID, NAME FROM PERSON WHERE ((ID > 4)) ORDER BY ID ASC  -> Eve
     * </pre>
     * (Syntaxe H2 : "TOP 2" ; sur PostgreSQL ou MySQL, ce serait "LIMIT 2".)
     * La page 3 ne contient qu'une ligne, moins que la taille de page : le reader sait que c'est
     * la fin et renvoie null sans lancer de 4e requête.
     *
     * <p>Les pages suivantes ne font pas "OFFSET n" (lent sur une grosse table) mais repartent de
     * la dernière valeur de la clé de tri ("WHERE ID > dernier ID lu"), d'où l'obligation d'avoir
     * une clé de tri <b>unique</b>.
     *
     * <p>La taille de page est un réglage du reader, indépendant de la taille du chunk.
     * On les prend égales ({@link #CHUNK_SIZE}) : une requête SQL par chunk.
     *
     * <p><b>Pourquoi {@code @StepScope} ?</b> Le reader a un état : la page en mémoire et la
     * position de lecture. Sans {@code @StepScope}, ce bean serait un singleton créé une seule
     * fois : au 2e lancement du job, il repartirait de là où le 1er s'était arrêté (la fin) et
     * ne lirait rien. Avec {@code @StepScope}, Spring crée un nouveau reader à chaque exécution
     * du step.
     *
     * <p><b>Pourquoi le type de retour est-il {@code JdbcPagingItemReader} et pas
     * {@code ItemReader} ?</b> Avec {@code @StepScope}, Spring injecte un proxy qui n'implémente
     * que le type déclaré. Ce reader implémente aussi {@code ItemStream} ({@code open},
     * {@code update}, {@code close}) : Spring Batch l'appelle pour ouvrir le reader et, avant
     * chaque commit, pour enregistrer sa position dans BATCH_STEP_EXECUTION_CONTEXT (reprise
     * après échec). Déclaré en {@code ItemReader}, le proxy n'implémenterait pas
     * {@code ItemStream} et la lecture échouerait ({@code ReaderNotOpenException}).
     *
     * @param dataSource la base H2, fournie par Spring Boot (la même que celle des tables BATCH_*)
     */
    @Bean
    @StepScope
    public JdbcPagingItemReader<String> namesReader(DataSource dataSource) throws Exception {
        // La requête est décrite en morceaux (SELECT, FROM, clé de tri) : c'est le
        // "query provider" qui assemble la requête de chaque page, dans le dialecte SQL
        // de la base (détecté à partir de la DataSource : ici H2).
        SqlPagingQueryProviderFactoryBean queryProvider = new SqlPagingQueryProviderFactoryBean();
        queryProvider.setDataSource(dataSource);
        queryProvider.setSelectClause("SELECT ID, NAME");
        queryProvider.setFromClause("FROM PERSON");
        queryProvider.setSortKey("ID");

        JdbcPagingItemReader<String> reader = new JdbcPagingItemReader<>();
        reader.setDataSource(dataSource);
        reader.setQueryProvider(queryProvider.getObject());
        reader.setPageSize(CHUNK_SIZE);
        // RowMapper : transforme une ligne du ResultSet en élément (ici, juste le nom).
        reader.setRowMapper((resultSet, rowNum) -> resultSet.getString("NAME"));
        // Pas besoin d'appeler reader.afterPropertiesSet() : comme c'est un bean,
        // Spring l'appelle lui-même après la création.
        return reader;
    }

    /**
     * ItemProcessor (optionnel) : transforme un élément lu en élément à écrire.
     *
     * <p>C'est l'endroit pour la logique métier : validation, conversion, enrichissement...
     * Si le processor renvoie {@code null}, l'élément est <i>filtré</i> : il ne sera pas écrit
     * (et le compteur "filterCount" du step est incrémenté).
     *
     * <p>Ce n'est pas un bean : il n'a pas d'état, on peut donc le créer directement.
     */
    private ItemProcessor<String, String> greetingProcessor() {
        return name -> "Hello, " + name.toUpperCase() + "!";
    }

    /**
     * ItemWriter : écrit un chunk entier en une fois.
     *
     * <p>Contrairement au reader et au processor qui travaillent élément par élément,
     * le writer reçoit une {@code List} contenant tous les éléments du chunk. Cela permet
     * d'écrire efficacement, par exemple avec un INSERT en batch JDBC.
     * (En Spring Batch 5, cette liste est remplacée par un objet {@code Chunk}.)
     *
     * <p>Dans un vrai projet : {@code FlatFileItemWriter} (fichier), {@code JdbcBatchItemWriter}
     * (base de données), {@code JpaItemWriter}...
     */
    private ItemWriter<String> consoleWriter() {
        return items -> {
            System.out.println("--- writing chunk of " + items.size() + " item(s)");
            items.forEach(greeting -> System.out.println("    " + greeting));
        };
    }

    // =====================================================================
    // Le Job
    // =====================================================================

    /**
     * Le Job assemble les steps et définit leur ordre d'exécution.
     *
     * <p>Les steps sont injectés par nom de paramètre : {@code helloStep} et
     * {@code greetStep} correspondent aux noms des méthodes {@code @Bean} ci-dessus.
     *
     * <p>{@code start(...).next(...)} : exécution séquentielle. Si un step échoue,
     * le job s'arrête en statut FAILED et les steps suivants ne sont pas exécutés.
     * (Spring Batch permet aussi des flux conditionnels : {@code .on("FAILED").to(...)}.)
     */
    @Bean
    public Job helloJob(Step helloStep, Step greetStep) {
        // "helloJob" est le nom du job, enregistré dans la table BATCH_JOB_INSTANCE.
        return jobBuilderFactory.get("helloJob")
                .start(helloStep)
                .next(greetStep)
                .build();
    }
}
