# Concurrent Ticket Booking Engine — Java 21

Projet final de multithreading, conforme au support du professeur (Parties A et B, phases 1 à 3, défi deadlock). L'application est **en console** ; aucune interface graphique n'est demandée dans le sujet.

## Prérequis
- JDK 21 ou plus récent (`java -version`)
- Maven 3.9+ (`mvn -version`) pour JUnit 5
- IntelliJ IDEA ou Eclipse (facultatif)

## Importation
Ouvrir le dossier `ticket-booking-java21` (celui qui contient `pom.xml`) comme projet Maven. Définir le JDK du projet sur 21.

## Exécution avec Maven
```bash
mvn clean test
mvn compile exec:java -Dexec.args="race"
mvn compile exec:java -Dexec.args="fixes"
mvn compile exec:java -Dexec.args="pipeline"
mvn compile exec:java -Dexec.args="platform"
mvn compile exec:java -Dexec.args="virtual"
mvn compile exec:java -Dexec.args="deadlock"
```

## Exécution sans Maven (application seulement)
Linux / macOS :
```bash
mkdir -p out
javac --release 21 -d out src/main/java/com/concert/*.java
java -cp out com.concert.Main race
java -cp out com.concert.Main fixes
java -cp out com.concert.Main pipeline
java -cp out com.concert.Main platform
java -cp out com.concert.Main virtual
java -cp out com.concert.Main deadlock
```
Windows PowerShell :
```powershell
New-Item -ItemType Directory -Force out
javac --release 21 -d out src/main/java/com/concert/*.java
java -cp out com.concert.Main pipeline
```
Les tests JUnit nécessitent Maven (ou les dépendances JUnit installées séparément).

## Architecture réelle
```text
src/main/java/com/concert/
  Model.java                 Seat, SeatState, BookingRequest, BookingResult,
                             Confirmed, Rejected, Expired, BookingEngine
  Engines.java               BaseEngine, NaiveBookingEngine,
                             SynchronizedBookingEngine, LockBookingEngine,
                             AtomicBookingEngine
  PaymentGateway.java        Paiement simulé
  PipelineBookingEngine.java Holds, queue bornée, workers, scheduler,
                             compteurs, shutdown
  DeadlockDemo.java          Deadlock volontaire + échange ordonné
  Main.java                  CLI, stress et mesures
src/test/java/com/concert/
  BookingTest.java           Tests JUnit 5
```

## Déroulement
1. **Phase 1** : `race` prouve le double-booking avec un test déterministe à 2 clients et 5 essais de stress (1 000 tâches / 200 places). `fixes` vérifie les 3 corrections, 20 fois chacune.
2. **Phase 2** : `pipeline` crée un hold atomique `FREE -> HELD`, met le paiement dans `ArrayBlockingQueue`, le traite avec 16 workers, expire le hold au bout de 2 s et vérifie les invariants. Queue pleine : attente de 100 ms puis rejet `busy`. Statistiques affichées chaque seconde.
3. **Phase 3** : `platform` lance 10 000 demandes avec 50 platform threads ; `virtual` lance 10 000 demandes avec `newVirtualThreadPerTaskExecutor`. Le programme mesure durée, pic de clients actifs et pic de threads JVM **platform** via ThreadMXBean (les virtual threads ne sont pas comptés par cet indicateur).
4. **Deadlock** : `deadlock` affiche un thread dump JVM du cycle volontaire. `swapSafe()` évite l'interblocage en verrouillant les places dans l'ordre croissant.

## Invariants importants
- `SOLD == confirmed` après la fin de toutes les demandes.
- `confirmed + rejected + expired == requests` après la fin de toutes les demandes.
- Pas de double confirmation d'une même place.
- Un paiement tardif ne confirme pas un hold expiré (identité du hold vérifiée sous verrou).
- `shutdown()` ferme l'admission, attend les workers, résout les holds restants, puis arrête scheduler et reporter.

## Précautions de mesure
Les résultats sont **non déterministes** (ordonnancement, charge machine, échecs de paiement aléatoires). Pour comparer les performances, répéter plusieurs fois et calculer la moyenne. Les deux tests de charge utilisent le même nombre de clients mais leurs confirmations peuvent différer, car la contention et les paiements aléatoires changent. `peakJvmThreads` mesure des threads platform présents dans la JVM, pas le nombre de virtual threads.

## Pièces à remettre
- Le présent code source (ZIP ou dépôt Git)
- Les tests JUnit et le README
- `REPONSES_QUIZ.md` : Partie A, à reformuler **individuellement** selon le professeur
- `RAPPORT_A_COMPLETER.md` : canevas du rapport d'une page ; remplacer les exemples par les résultats de votre propre ordinateur
- `docs/deadlock_dump_exemple.txt` : extrait d'une démonstration de deadlock

## Limites et bonus
Le sujet décrit `PaymentGateway` comme fourni par l'enseignant : faute de fichier séparé, une simulation compatible avec le scénario a été incluse. Les bonus `CompletableFuture.orTimeout`, `StructuredTaskScope` et journal ordonné ne sont **pas implémentés** (ils sont facultatifs). Le moteur utilise néanmoins `CompletableFuture` pour attendre les résultats de paiement.


## Organisation des classes (version séparée)
Chaque type principal du modèle et chaque moteur de la phase 1 possède
désormais son propre fichier `.java`. Le package `com.concert` est conservé
pour éviter toute modification de la logique ou des imports existants.
Les types internes (`Snapshot`, `Hold`, `PaymentTask`, `Result`) restent
dans leur classe englobante, car ils en font partie.


## Mode interactif (terminal)

```bash
mvn clean compile
java -cp target/classes com.concert.Main interactive
```

Menu : consulter les places, réserver une place, lancer deux clients concurrents,
afficher les statistiques, tester un paiement refusé et une expiration après
2 secondes, puis arrêter proprement le moteur. Les tests de paiement refusé et
d'expiration utilisent un moteur isolé afin de préserver les 200 places du menu.

Les autres modes (`race`, `fixes`, `pipeline`, `platform`, `virtual`, `deadlock`)
restent disponibles.
# ticket-booking-java
