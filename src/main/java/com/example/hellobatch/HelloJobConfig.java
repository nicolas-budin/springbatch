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
 * Un Job = une suite de Steps.
 *
 *   helloJob
 *    ├── helloStep   (Tasklet : une action unique)
 *    └── greetStep   (Chunk   : lecture -> traitement -> écriture, par paquets)
 */
@Configuration
public class HelloJobConfig {

    // ---------- Step 1 : Tasklet ----------
    // Une Tasklet exécute une seule action. Idéal pour du "one-shot"
    // (nettoyer un dossier, appeler un service, ... ou dire bonjour).
    @Bean
    public Step helloStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("helloStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    System.out.println(">>> Hello World from Spring Batch!");
                    return RepeatStatus.FINISHED; // CONTINUABLE => la tasklet serait rappelée
                }, transactionManager)
                .build();
    }

    // ---------- Step 2 : Chunk (Reader -> Processor -> Writer) ----------
    // Le modèle principal de Spring Batch : on lit les éléments un par un,
    // on les transforme, puis on les écrit par paquets ("chunks") de N.
    // Chaque chunk est traité dans sa propre transaction.
    @Bean
    public Step greetStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                          ListItemReader<String> namesReader) {
        return new StepBuilder("greetStep", jobRepository)
                .<String, String>chunk(2, transactionManager)   // commit tous les 2 éléments
                .reader(namesReader)
                .processor(greetingProcessor())
                .writer(consoleWriter())
                .build();
    }

    // Reader : fournit les éléments un par un ; renvoie null quand c'est fini.
    // (En vrai : FlatFileItemReader pour un CSV, JdbcCursorItemReader pour une BDD...)
    // @StepScope : un nouveau reader est créé à chaque exécution du step.
    // Sans ça, le reader (qui a un état) serait déjà "vide" au 2e lancement du job.
    @Bean
    @StepScope
    public ListItemReader<String> namesReader() {
        return new ListItemReader<>(List.of("Alice", "Bob", "Charlie", "Diane", "Eve"));
    }

    // Processor (optionnel) : transforme un élément. Renvoyer null = filtrer l'élément.
    private ItemProcessor<String, String> greetingProcessor() {
        return name -> "Hello, " + name.toUpperCase() + "!";
    }

    // Writer : reçoit un chunk entier (ici 2 éléments max) d'un coup.
    private ItemWriter<String> consoleWriter() {
        return chunk -> {
            System.out.println("--- writing chunk of " + chunk.size() + " item(s)");
            chunk.forEach(greeting -> System.out.println("    " + greeting));
        };
    }

    // ---------- Le Job ----------
    @Bean
    public Job helloJob(JobRepository jobRepository, Step helloStep, Step greetStep) {
        return new JobBuilder("helloJob", jobRepository)
                .start(helloStep)
                .next(greetStep)
                .build();
    }
}
