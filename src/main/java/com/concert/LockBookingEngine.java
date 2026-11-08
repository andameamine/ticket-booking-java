package com.concert;

import java.util.concurrent.locks.*;

class LockBookingEngine extends BaseEngine {
    private final boolean[] sold;
    private final ReentrantLock[] locks;
    LockBookingEngine(int n) { super(n); sold=new boolean[n]; locks=new ReentrantLock[n]; for(int i=0;i<n;i++)locks[i]=new ReentrantLock(); }
    public BookingResult book(BookingRequest r) {
        if (!valid(r)) return new Rejected("invalid");
        int id=r.seatId(); locks[id].lock();
        try { if(sold[id]) return new Rejected("taken"); sold[id]=true; return new Confirmed(id,r.customerId()); }
        finally { locks[id].unlock(); }
    }
    public int soldCount() { int n=0; for(int i=0;i<seats;i++) { locks[i].lock(); try { if(sold[i]) n++; } finally { locks[i].unlock(); } } return n; }
}
