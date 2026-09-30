-- Exécuté automatiquement par Spring Boot au démarrage (fichier "schema.sql" à la racine
-- du classpath), avant data.sql. Crée la table métier lue par le reader de greetStep.
-- (Les tables BATCH_* de Spring Batch sont créées à part, par un autre script.)
--
-- "IF NOT EXISTS" : dans les tests, plusieurs contextes Spring partagent la même base H2
-- en mémoire ; le script peut donc être exécuté plusieurs fois sur la même base.
CREATE TABLE IF NOT EXISTS PERSON (
    ID   BIGINT       PRIMARY KEY,
    NAME VARCHAR(100) NOT NULL
);
