package com.concert;

import java.util.Scanner;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/** Terminal-only interactive validation; the original demos remain unchanged. */
public final class InteractiveMenu {
    private static final int SEATS = 200;
    private static final Scanner IN = new Scanner(System.in);
    private static final AtomicInteger CUSTOMER_IDS = new AtomicInteger();
    private InteractiveMenu() {}

    public static void run() throws Exception {
        System.out.println("\n=== CONCURRENT TICKET BOOKING — INTERACTIVE MODE ===");
        System.out.println("Seats: 0 to 199 | Hold timeout: 2 seconds");
        try (PipelineBookingEngine engine = new PipelineBookingEngine(SEATS, 8, 64,
                PaymentGateway.simulated(), false)) {
            while (true) {
                System.out.println("\n1. Show seat map (FREE / HELD / SOLD)");
                System.out.println("2. Book a seat (simulated payment)");
                System.out.println("3. Simulate 2 clients on the same seat");
                System.out.println("4. Show statistics");
                System.out.println("5. Test guaranteed payment failure (separate test engine)");
                System.out.println("6. Test expiration after 2 seconds (separate test engine)");
                System.out.println("7. Exit (clean shutdown)");
                int choice = askInt("Choose: ");
                switch (choice) {
                    case 1 -> showSeats(engine);
                    case 2 -> bookSeat(engine);
                    case 3 -> twoClients(engine);
                    case 4 -> stats(engine);
                    case 5 -> paymentFailureTest();
                    case 6 -> expirationTest();
                    case 7 -> { System.out.println("Shutting down workers and scheduler..."); return; }
                    default -> System.out.println("Unknown option.");
                }
            }
        }
    }

    private static int askInt(String label) {
        while (true) {
            System.out.print(label);
            String line = IN.nextLine().trim();
            try { return Integer.parseInt(line); }
            catch (NumberFormatException ex) { System.out.println("Enter an integer."); }
        }
    }
    private static int seatId() { return askInt("Seat ID (0-199): "); }
    private static void showSeats(PipelineBookingEngine engine) {
        System.out.println("Legend: . FREE | H HELD | X SOLD");
        for (int i=0;i<engine.seatCount();i++) {
            SeatState state = engine.seatState(i);
            System.out.print(state == SeatState.FREE ? ". " : state == SeatState.HELD ? "H " : "X ");
            if ((i+1)%20==0) System.out.printf("  [%03d-%03d]%n",i-19,i);
        }
    }
    private static void bookSeat(PipelineBookingEngine engine) {
        int id = seatId();
        System.out.print("Customer name: ");
        String customer = IN.nextLine().trim();
        if(customer.isEmpty()) customer = "customer-" + CUSTOMER_IDS.incrementAndGet();
        BookingResult result = engine.book(new BookingRequest(customer,id));
        System.out.println("Booking result: " + result);
        if (id>=0 && id<SEATS) System.out.println("Seat " + id + " now: " + engine.seatState(id));
    }
    private static void twoClients(PipelineBookingEngine engine) throws Exception {
        int id = seatId();
        if (id<0 || id>=SEATS) { System.out.println("Invalid seat ID."); return; }
        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            CountDownLatch start = new CountDownLatch(1);
            Future<BookingResult> a = executor.submit(() -> {start.await();return engine.book(new BookingRequest("A",id));});
            Future<BookingResult> b = executor.submit(() -> {start.await();return engine.book(new BookingRequest("B",id));});
            start.countDown();
            BookingResult ra=a.get(),rb=b.get();
            System.out.println("Client A: " + ra);
            System.out.println("Client B: " + rb);
            System.out.println("Seat " + id + ": " + engine.seatState(id));
            int confirmations=(ra instanceof Confirmed?1:0)+(rb instanceof Confirmed?1:0);
            System.out.println(confirmations<=1?"PASS: no double sale":"FAIL: double sale");
        }
    }
    private static void stats(PipelineBookingEngine engine) {
        System.out.println("Stats: " + engine.snapshot());
        System.out.println("Sold: " + engine.soldCount() + " / " + SEATS);
        System.out.println("Payments queued: " + engine.queuedPayments());
    }
    private static void paymentFailureTest() {
        try (PipelineBookingEngine test = new PipelineBookingEngine(1,1,2, customer -> false,false)) {
            BookingResult result=test.book(new BookingRequest("failure-demo",0));
            System.out.println("Payment result: " + result + " | Seat state: " + test.seatState(0));
            System.out.println(result instanceof Rejected && test.seatState(0)==SeatState.FREE ?
                    "PASS: failed payment released seat" : "FAIL: unexpected result");
        }
    }
    private static void expirationTest() {
        try (PipelineBookingEngine test = new PipelineBookingEngine(1,1,2, customer -> {
            Thread.sleep(2500); return true;
        },false)) {
            BookingResult result=test.book(new BookingRequest("slow-payment-demo",0));
            System.out.println("After 2s timeout: " + result + " | Seat state: " + test.seatState(0));
            System.out.println(result instanceof Expired && test.seatState(0)==SeatState.FREE ?
                    "PASS: expired hold released seat" : "FAIL: unexpected result");
            try { Thread.sleep(700); } catch (InterruptedException ex) { Thread.currentThread().interrupt(); }
            System.out.println("After late payment: " + test.seatState(0));
            System.out.println(test.seatState(0)==SeatState.FREE && test.soldCount()==0 ?
                    "PASS: late payment cannot sell expired seat" : "FAIL: late sale");
        }
    }
}
