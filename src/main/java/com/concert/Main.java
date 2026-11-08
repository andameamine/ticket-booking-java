package com.concert;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/** Console demonstrations required by the instructor. */
public final class Main {
    private Main() {}

    public static void main(String[] args) throws Exception {
        String mode = args.length == 0 ? "help" : args[0];
        switch (mode) {
            case "interactive" -> InteractiveMenu.run();
            case "race" -> {
                demonstrateGuaranteedRace();
                for (int round = 1; round <= 5; round++) {
                    Result result = stress(new NaiveBookingEngine(200), 1000, false);
                    System.out.printf("Naive round %d: %s%n", round, result);
                    if (result.duplicates() == 0) System.out.println("No duplicate observed this round; race conditions are scheduling-dependent.");
                }
            }
            case "fixes" -> {
                for (int round = 1; round <= 20; round++) {
                    for (String kind : List.of("synchronized", "locks", "atomic")) {
                        BookingEngine engine = switch (kind) {
                            case "synchronized" -> new SynchronizedBookingEngine(200);
                            case "locks" -> new LockBookingEngine(200);
                            default -> new AtomicBookingEngine(200);
                        };
                        Result result = stress(engine, 1000, false);
                        if (result.confirmed() != 200 || result.duplicates() != 0 || result.sold() != 200)
                            throw new AssertionError("Incorrect " + kind + ": " + result);
                        if (round == 1) System.out.println(kind + " => " + result);
                    }
                }
                System.out.println("PASS: all three fixed engines passed 20 consecutive rounds.");
            }
            case "pipeline" -> pipeline(1000, false);
            case "platform" -> pipeline(10000, false);
            case "virtual" -> pipeline(10000, true);
            case "deadlock" -> DeadlockDemo.demonstrate();
            default -> System.out.println("Usage: interactive | race | fixes | pipeline | platform | virtual | deadlock");
        }
    }

    private static void demonstrateGuaranteedRace() throws Exception {
        NaiveBookingEngine engine = new NaiveBookingEngine(1, new CyclicBarrier(2));
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<BookingResult> a = pool.submit(() -> engine.book(new BookingRequest("A", 0)));
            Future<BookingResult> b = pool.submit(() -> engine.book(new BookingRequest("B", 0)));
            BookingResult ra = a.get(3, TimeUnit.SECONDS), rb = b.get(3, TimeUnit.SECONDS);
            System.out.println("DETERMINISTIC RACE: " + ra + " / " + rb + " | physically sold=" + engine.soldCount());
            if (!(ra instanceof Confirmed) || !(rb instanceof Confirmed)) throw new AssertionError("Expected two confirmations");
        } finally { pool.shutdownNow(); }
    }

    /** peakJvmThreads counts live platform threads, NOT virtual threads. */
    record Result(int confirmed, int duplicates, int sold, long millis,
                  int peakActiveClients, int peakJvmThreads) {}

    static Result stress(BookingEngine engine, int clients, boolean virtual) throws Exception {
        try {
            return runLoad(engine, clients, virtual);
        } finally {
            engine.shutdown();
        }
    }

    static void pipeline(int clients, boolean virtual) throws Exception {
        try (PipelineBookingEngine engine = new PipelineBookingEngine(200, 16, 100, PaymentGateway.simulated(), true)) {
            Result result = runLoad(engine, clients, virtual);
            PipelineBookingEngine.Snapshot stats = engine.snapshot();
            System.out.println((virtual ? "VIRTUAL" : "PLATFORM") + " clients=" + clients + " " + result);
            System.out.println("STATS: " + stats);
            if (result.duplicates() != 0 || engine.soldCount() != stats.confirmed() || stats.total() != clients)
                throw new AssertionError("Pipeline invariants violated");
            System.out.println("PASS: SOLD = CONFIRMED and CONFIRMED + REJECTED + EXPIRED = requests");
        }
    }

    static Result runLoad(BookingEngine engine, int clients, boolean virtual) throws Exception {
        ExecutorService executor = virtual ? Executors.newVirtualThreadPerTaskExecutor() : Executors.newFixedThreadPool(50);
        ScheduledExecutorService monitor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "platform-thread-monitor"); t.setDaemon(true); return t;
        });
        ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        AtomicInteger peakJvm = new AtomicInteger(bean.getThreadCount());
        AtomicInteger active = new AtomicInteger(), peakActive = new AtomicInteger();
        ConcurrentHashMap<Integer, AtomicInteger> confirmations = new ConcurrentHashMap<>();
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>(clients);
        long begin = System.nanoTime();
        monitor.scheduleAtFixedRate(() -> peakJvm.accumulateAndGet(bean.getThreadCount(), Math::max), 0, 2, TimeUnit.MILLISECONDS);
        try {
            for (int i = 0; i < clients; i++) {
                final int index = i;
                futures.add(executor.submit(() -> {
                    try {
                        start.await();
                        peakActive.accumulateAndGet(active.incrementAndGet(), Math::max);
                        try {
                            BookingResult outcome = engine.book(new BookingRequest("client-" + index, index % 200));
                            if (outcome instanceof Confirmed c)
                                confirmations.computeIfAbsent(c.seatId(), k -> new AtomicInteger()).incrementAndGet();
                        } finally { active.decrementAndGet(); }
                    } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                }));
            }
            start.countDown();
            for (Future<?> future : futures) future.get(60, TimeUnit.SECONDS);
        } finally {
            start.countDown();
            executor.shutdown();
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) executor.shutdownNow();
            monitor.shutdownNow();
            monitor.awaitTermination(2, TimeUnit.SECONDS);
        }
        int confirmed = confirmations.values().stream().mapToInt(AtomicInteger::get).sum();
        int duplicates = confirmations.values().stream().mapToInt(c -> Math.max(0, c.get() - 1)).sum();
        return new Result(confirmed, duplicates, engine.soldCount(),
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin), peakActive.get(), peakJvm.get());
    }
}
