package com.concert;


abstract class BaseEngine implements BookingEngine {
    final int seats;
    BaseEngine(int seats) { this.seats = seats; }
    boolean valid(BookingRequest r) { return r != null && r.customerId() != null && r.seatId() >= 0 && r.seatId() < seats; }
    public void shutdown() {}
}
// Intentionally incorrect: for classroom demonstration only.
