package com.example.hellobatch;

import java.util.List;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

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

    // =====================================================================
    // Step 1 : Tasklet
    // =====================================================================

    /**
     * Step de type Tasklet : exécute une seule action.
     *
     * @param jobRepository      fourni par Spring Boot ; il enregistre en base l'état
     *                           du step (démarré, terminé, en échec...)
     * @param transactionManager fourni par Spring Boot ; l'exécution de la tasklet
     *                           est entourée d'une transaction
     */
    @Bean
    public Step helloStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        // "helloStep" est le nom du step : c'est ce nom qui apparaît dans les logs
        // et dans la table BATCH_STEP_EXECUTION.
        return new StepBuilder("helloStep", jobRepository)
                // Une Tasklet est une interface fonctionnelle : on peut l'écrire en lambda.
                //  - contribution : permet de mettre à jour les compteurs du step (lectures, écritures...)
                //  - chunkContext : donne accès au contexte d'exécution (paramètres du job, etc.)
                .tasklet((contribution, chunkContext) -> {
                    System.out.println(">>> Hello World from Spring Batch!");
                    // FINISHED    => la tasklet a terminé, on passe à la suite.
                    // CONTINUABLE => Spring Batch rappellerait la tasklet (utile pour boucler).
                    return RepeatStatus.FINISHED;
                }, transactionManager)
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
    public Step greetStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                          ListItemReader<String> namesReader) {
        return new StepBuilder("greetStep", jobRepository)
                // <String, String> : type lu par le reader, type écrit par le writer.
                // 2 : taille du chunk = nombre d'éléments par transaction (commit interval).
                //     En production on prend plutôt 100, 500, 1000... selon le volume.
                .<String, String>chunk(2, transactionManager)
                .reader(namesReader)
                .processor(greetingProcessor())
                .writer(consoleWriter())
                .build();
    }

    /**
     * ItemReader : fournit les éléments un par un.
     *
     * <p>Contrat : chaque appel à {@code read()} renvoie l'élément suivant,
     * et renvoie {@code null} quand il n'y a plus rien à lire (fin du step).
     *
     * <p>Ici on lit une simple liste en mémoire. Dans un vrai projet on utiliserait
     * par exemple {@code FlatFileItemReader} (fichier CSV), {@code JdbcCursorItemReader}
     * ou {@code JdbcPagingItemReader} (base de données), {@code JsonItemReader}...
     *
     * <p><b>Pourquoi {@code @StepScope} ?</b> Un reader a un état : {@code ListItemReader}
     * retire les éléments au fur et à mesure qu'il les lit. Sans {@code @StepScope},
     * ce bean serait un singleton créé une seule fois : au 2e lancement du job, la liste
     * serait déjà vide et le step ne lirait rien. Avec {@code @StepScope}, Spring crée
     * un nouveau reader à chaque exécution du step.
     * ({@code @StepScope} permet aussi d'injecter des paramètres du job, par exemple
     * {@code @Value("#{jobParameters['fichier']}")}.)
     */
    @Bean
    @StepScope
    public ListItemReader<String> namesReader() {
        return new ListItemReader<>(List.of("Alice", "Bob", "Charlie", "Diane", "Eve"));
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
     * le writer reçoit un {@code Chunk} (une liste d'éléments). Cela permet d'écrire
     * efficacement, par exemple avec un INSERT en batch JDBC.
     *
     * <p>Dans un vrai projet : {@code FlatFileItemWriter} (fichier), {@code JdbcBatchItemWriter}
     * (base de données), {@code JpaItemWriter}...
     */
    private ItemWriter<String> consoleWriter() {
        return chunk -> {
            System.out.println("--- writing chunk of " + chunk.size() + " item(s)");
            chunk.forEach(greeting -> System.out.println("    " + greeting));
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
    public Job helloJob(JobRepository jobRepository, Step helloStep, Step greetStep) {
        // "helloJob" est le nom du job, enregistré dans la table BATCH_JOB_INSTANCE.
        return new JobBuilder("helloJob", jobRepository)
                .start(helloStep)
                .next(greetStep)
                .build();
    }
}
