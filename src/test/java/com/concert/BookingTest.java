package com.concert;

import org.junit.jupiter.api.Test;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

class BookingTest {
    @Test void naiveEngineDuplicatesSeatDeterministically() throws Exception {
        NaiveBookingEngine engine = new NaiveBookingEngine(1, new CyclicBarrier(2));
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<BookingResult> first = pool.submit(() -> engine.book(new BookingRequest("A", 0)));
            Future<BookingResult> second = pool.submit(() -> engine.book(new BookingRequest("B", 0)));
            assertInstanceOf(Confirmed.class, first.get(3, TimeUnit.SECONDS));
            assertInstanceOf(Confirmed.class, second.get(3, TimeUnit.SECONDS));
            assertEquals(1, engine.soldCount()); // Two confirmations for one physical seat!
        } finally { pool.shutdownNow(); }
    }

    @Test void allThreeFixesPassTwentyTimes() throws Exception {
        for (int round = 0; round < 20; round++) {
            for (BookingEngine engine : new BookingEngine[]{
                    new SynchronizedBookingEngine(200), new LockBookingEngine(200), new AtomicBookingEngine(200)}) {
                Main.Result result = Main.stress(engine, 1000, false);
                assertEquals(200, result.confirmed());
                assertEquals(200, result.sold());
                assertEquals(0, result.duplicates());
            }
        }
    }

    @Test void pipelineSuccessFailureAndTakenSeat() {
        try (PipelineBookingEngine engine = new PipelineBookingEngine(2, 2, 5, id -> !id.equals("bad"), false)) {
            assertInstanceOf(Confirmed.class, engine.book(new BookingRequest("good", 0)));
            assertInstanceOf(Rejected.class, engine.book(new BookingRequest("again", 0)));
            assertInstanceOf(Rejected.class, engine.book(new BookingRequest("bad", 1)));
            assertEquals(1, engine.soldCount());
            assertEquals(3, engine.snapshot().total());
            assertEquals(1, engine.snapshot().paymentFailures());
        }
    }

    @Test void expiredPaymentNeverSells() {
        try (PipelineBookingEngine engine = new PipelineBookingEngine(1, 1, 2,
                id -> { Thread.sleep(200); return true; }, false, 50)) {
            assertInstanceOf(Expired.class, engine.book(new BookingRequest("late", 0)));
            assertEquals(0, engine.soldCount());
        }
    }

    @Test void fullQueueRejectsBusyWithoutBlockingForever() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try (PipelineBookingEngine engine = new PipelineBookingEngine(3, 1, 1, id -> {
            entered.countDown(); release.await(2, TimeUnit.SECONDS); return true;
        }, false, 1500)) {
            ExecutorService clients = Executors.newFixedThreadPool(3);
            try {
                Future<BookingResult> first = clients.submit(() -> engine.book(new BookingRequest("a", 0)));
                assertTrue(entered.await(1, TimeUnit.SECONDS));
                Future<BookingResult> second = clients.submit(() -> engine.book(new BookingRequest("b", 1)));
                // Wait until the queued hold is observable before submitting the third request.
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
                while (engine.queuedPayments() == 0 && System.nanoTime() < deadline) Thread.yield();
                assertEquals(1, engine.queuedPayments());
                Future<BookingResult> third = clients.submit(() -> engine.book(new BookingRequest("c", 2)));
                BookingResult result = third.get(2, TimeUnit.SECONDS);
                assertTrue(result instanceof Rejected || result instanceof Expired);
                release.countDown();
                first.get(3, TimeUnit.SECONDS); second.get(3, TimeUnit.SECONDS);
                assertEquals(3, engine.snapshot().total());
                assertEquals(engine.snapshot().confirmed(), engine.soldCount());
            } finally { release.countDown(); clients.shutdownNow(); }
        }
    }

    @Test void shutdownRejectsNewRequests() {
        PipelineBookingEngine engine = new PipelineBookingEngine(1, 1, 1, id -> true, false);
        engine.shutdown();
        assertInstanceOf(Rejected.class, engine.book(new BookingRequest("late", 0)));
    }

    @Test void reverseOrderSwapCannotDeadlock() throws Exception {
        DeadlockDemo demo = new DeadlockDemo();
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> a = executor.submit(() -> demo.swapSafe(0, 1, () -> {}));
            Future<?> b = executor.submit(() -> demo.swapSafe(1, 0, () -> {}));
            a.get(2, TimeUnit.SECONDS); b.get(2, TimeUnit.SECONDS);
        } finally { executor.shutdownNow(); }
    }

    @Test void virtualThreadsCanBookWithoutDuplicates() throws Exception {
        Main.Result result = Main.stress(new AtomicBookingEngine(200), 10000, true);
        assertEquals(200, result.confirmed());
        assertEquals(0, result.duplicates());
    }
}
