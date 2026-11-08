package com.concert;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;
import java.util.*;

final class PipelineBookingEngine implements BookingEngine {
    private record Hold(long token, String customer, long createdAt, CompletableFuture<BookingResult> outcome) {}
    private record PaymentTask(int seat, Hold hold) {}
    record Snapshot(long confirmed,long rejected,long expired,long paymentFailures) {
        long total() { return confirmed+rejected+expired; }
    }
    private final ReentrantLock[] locks;
    private final SeatState[] states;
    private final Hold[] holds;
    private final BlockingQueue<PaymentTask> queue;
    private final ExecutorService workers;
    private final ScheduledExecutorService scheduler=Executors.newSingleThreadScheduledExecutor();
    private final ScheduledExecutorService reporter=Executors.newSingleThreadScheduledExecutor();
    private final PaymentGateway gateway;
    private final AtomicLong ids=new AtomicLong();
    private final LongAdder confirmed=new LongAdder(),rejected=new LongAdder(),expired=new LongAdder(),failures=new LongAdder();
    private final ReentrantReadWriteLock lifecycle=new ReentrantReadWriteLock();
    private volatile boolean closed=false;
    private final long holdNanos;
    PipelineBookingEngine(int seatCount,int workerCount,int queueSize,PaymentGateway gateway,boolean reporting) {
        this(seatCount,workerCount,queueSize,gateway,reporting,2000);
    }
    PipelineBookingEngine(int seatCount,int workerCount,int queueSize,PaymentGateway gateway,boolean reporting,long holdMillis) {
        if(seatCount<=0||workerCount<=0||queueSize<=0||holdMillis<=0)throw new IllegalArgumentException();
        this.gateway=Objects.requireNonNull(gateway);
        holdNanos=TimeUnit.MILLISECONDS.toNanos(holdMillis);
        locks=new ReentrantLock[seatCount]; states=new SeatState[seatCount]; holds=new Hold[seatCount];
        for(int i=0;i<seatCount;i++){locks[i]=new ReentrantLock();states[i]=SeatState.FREE;}
        queue=new ArrayBlockingQueue<>(queueSize);
        workers=Executors.newFixedThreadPool(workerCount);
        for(int i=0;i<workerCount;i++)workers.submit(this::consume);
        scheduler.scheduleAtFixedRate(this::expireHolds,50,50,TimeUnit.MILLISECONDS);
        if(reporting)reporter.scheduleAtFixedRate(()->System.out.println("[STATS] "+snapshot()+" SOLD="+soldCount()),1,1,TimeUnit.SECONDS);
    }
    private BookingResult reject(String why) { rejected.increment(); return new Rejected(why); }
    public BookingResult book(BookingRequest request) {
        if(request==null||request.customerId()==null||request.seatId()<0||request.seatId()>=states.length)return reject("invalid");
        Hold h;
        lifecycle.readLock().lock();
        try {
            if(closed)return reject("shutdown");
            int id=request.seatId(); locks[id].lock();
            try {
                if(states[id]!=SeatState.FREE)return reject("taken");
                h=new Hold(ids.incrementAndGet(),request.customerId(),System.nanoTime(),new CompletableFuture<>());
                states[id]=SeatState.HELD; holds[id]=h;
            } finally { locks[id].unlock(); }
            boolean queued;
            try { queued=queue.offer(new PaymentTask(id,h),100,TimeUnit.MILLISECONDS); }
            catch(InterruptedException e) { Thread.currentThread().interrupt(); queued=false; }
            if(!queued) { finish(id,h,new Rejected("busy"),false); }
        } finally { lifecycle.readLock().unlock(); }
        try { return h.outcome().get(); }
        catch(InterruptedException e) { Thread.currentThread().interrupt(); return h.outcome().join(); } // keep exactly one counted outcome
        catch(ExecutionException e) { throw new IllegalStateException(e); }
    }
    private void consume() {
        try {
            while(!Thread.currentThread().isInterrupted()) {
                PaymentTask t=queue.poll(100,TimeUnit.MILLISECONDS);
                if(t==null) { if(closed && queue.isEmpty())break; continue; }
                if(t.hold().outcome().isDone())continue;
                boolean success;
                try { success=gateway.charge(t.hold().customer()); }
                catch(InterruptedException e) { Thread.currentThread().interrupt(); finish(t.seat(),t.hold(),new Rejected("interrupted"),true); break; }
                catch(RuntimeException e) { success=false; }
                finish(t.seat(),t.hold(),success?new Confirmed(t.seat(),t.hold().customer()):new Rejected("payment failed"),!success);
            }
        } catch(InterruptedException e) { Thread.currentThread().interrupt(); }
    }
    private void finish(int id,Hold h,BookingResult result,boolean paymentFailure) {
        locks[id].lock();
        try {
            if(holds[id]!=h || h.outcome().isDone())return;
            if(System.nanoTime()-h.createdAt()>=holdNanos)result=new Expired(id);
            if(result instanceof Confirmed) { states[id]=SeatState.SOLD; confirmed.increment(); }
            else { states[id]=SeatState.FREE; if(result instanceof Expired)expired.increment(); else rejected.increment(); }
            if(paymentFailure)failures.increment();
            holds[id]=null;
            h.outcome().complete(result);
        } finally { locks[id].unlock(); }
    }
    private void expireHolds() {
        for(int i=0;i<states.length;i++) {
            locks[i].lock();
            try { Hold h=holds[i]; if(h!=null && System.nanoTime()-h.createdAt()>=holdNanos)finish(i,h,new Expired(i),false); }
            finally { locks[i].unlock(); }
        }
    }
    SeatState seatState(int seatId) {
        if (seatId < 0 || seatId >= states.length) throw new IllegalArgumentException("Invalid seat ID");
        locks[seatId].lock();
        try { return states[seatId]; }
        finally { locks[seatId].unlock(); }
    }
    int seatCount() { return states.length; }
    int queuedPayments() { return queue.size(); }
    public Snapshot snapshot() { return new Snapshot(confirmed.sum(),rejected.sum(),expired.sum(),failures.sum()); }
    public int soldCount() { int n=0; for(int i=0;i<states.length;i++){locks[i].lock();try{if(states[i]==SeatState.SOLD)n++;}finally{locks[i].unlock();}}return n; }
    public void shutdown() {
        lifecycle.writeLock().lock();
        try { if(closed)return; closed=true; }
        finally { lifecycle.writeLock().unlock(); }
        workers.shutdown();
        boolean interrupted=false;
        try { if(!workers.awaitTermination(15,TimeUnit.SECONDS)) { workers.shutdownNow(); if(!workers.awaitTermination(3,TimeUnit.SECONDS)) System.err.println("Workers did not terminate"); } }
        catch(InterruptedException e) { interrupted=true; workers.shutdownNow(); }
        // Any pending holds are resolved so no book() remains blocked.
        for(int i=0;i<states.length;i++){locks[i].lock();try{Hold h=holds[i];if(h!=null)finish(i,h,new Rejected("shutdown"),false);}finally{locks[i].unlock();}}
        scheduler.shutdownNow();reporter.shutdownNow();
        try { scheduler.awaitTermination(2,TimeUnit.SECONDS); reporter.awaitTermination(2,TimeUnit.SECONDS); }
        catch(InterruptedException e) { interrupted=true; }
        if(interrupted)Thread.currentThread().interrupt();
    }
}
