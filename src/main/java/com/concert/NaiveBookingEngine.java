package com.concert;

import java.util.concurrent.*;

class NaiveBookingEngine extends BaseEngine {
    final boolean[] sold;
    private final CyclicBarrier demonstrationBarrier;
    NaiveBookingEngine(int n) { this(n, null); }
    NaiveBookingEngine(int n, CyclicBarrier demonstrationBarrier) { super(n); sold = new boolean[n]; this.demonstrationBarrier = demonstrationBarrier; }
    public BookingResult book(BookingRequest r) {
        if (!valid(r)) return new Rejected("invalid");
        if (sold[r.seatId()]) return new Rejected("taken");
        if (demonstrationBarrier != null) {
            try { demonstrationBarrier.await(2, TimeUnit.SECONDS); }
            catch (Exception e) { throw new IllegalStateException("Race demonstration synchronization failed", e); }
        }
        Thread.yield(); // widen the race window
        sold[r.seatId()] = true;
        return new Confirmed(r.seatId(), r.customerId());
    }
    public int soldCount() { int n=0; for (boolean b:sold) if(b)n++; return n; }
}
