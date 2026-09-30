-- Exécuté automatiquement par Spring Boot au démarrage, après schema.sql : les données lues par le job.
--
-- MERGE (spécifique à H2) au lieu de INSERT : insère la ligne si l'ID n'existe pas, la met à jour
-- sinon. Le script peut ainsi être rejoué sur la même base (cas des tests) sans erreur de doublon.
MERGE INTO PERSON (ID, NAME) KEY (ID) VALUES (1, 'Alice');
MERGE INTO PERSON (ID, NAME) KEY (ID) VALUES (2, 'Bob');
MERGE INTO PERSON (ID, NAME) KEY (ID) VALUES (3, 'Charlie');
MERGE INTO PERSON (ID, NAME) KEY (ID) VALUES (4, 'Diane');
MERGE INTO PERSON (ID, NAME) KEY (ID) VALUES (5, 'Eve');
