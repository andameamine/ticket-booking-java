package com.concert;


class SynchronizedBookingEngine extends BaseEngine {
    private final boolean[] sold;
    SynchronizedBookingEngine(int n) { super(n); sold = new boolean[n]; }
    public synchronized BookingResult book(BookingRequest r) {
        if (!valid(r)) return new Rejected("invalid");
        if(sold[r.seatId()]) return new Rejected("taken");
        sold[r.seatId()] = true;
        return new Confirmed(r.seatId(), r.customerId());
    }
    public synchronized int soldCount() { int n=0; for(boolean b:sold) if(b)n++; return n; }
}
