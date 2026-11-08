package com.concert;

import java.util.concurrent.atomic.*;

class AtomicBookingEngine extends BaseEngine {
    private final AtomicReferenceArray<SeatState> states;
    AtomicBookingEngine(int n) { super(n); states=new AtomicReferenceArray<>(n); for(int i=0;i<n;i++) states.set(i,SeatState.FREE); }
    public BookingResult book(BookingRequest r) {
        if (!valid(r)) return new Rejected("invalid");
        return states.compareAndSet(r.seatId(),SeatState.FREE,SeatState.SOLD) ? new Confirmed(r.seatId(),r.customerId()) : new Rejected("taken");
    }
    public int soldCount() { int n=0; for(int i=0;i<seats;i++) if(states.get(i)==SeatState.SOLD)n++; return n; }
}
