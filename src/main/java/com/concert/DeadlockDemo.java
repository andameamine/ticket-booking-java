package com.concert;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;
import java.lang.management.*;
final class DeadlockDemo {
    private final ReentrantLock[] locks={new ReentrantLock(),new ReentrantLock()};
    // Safe seat swap: acquire both locks in ascending seat-id order.
    void swapSafe(int a,int b,Runnable action) {
        if(a==b){action.run();return;}
        int first=Math.min(a,b),second=Math.max(a,b);
        locks[first].lock();try{locks[second].lock();try{action.run();}finally{locks[second].unlock();}}finally{locks[first].unlock();}
    }
    // Deliberate deadlock in daemon threads, so the demonstration cannot prevent JVM exit.
    static void demonstrate() throws Exception {
        Object a=new Object(),b=new Object();CountDownLatch both=new CountDownLatch(2);
        Runnable left=()->{synchronized(a){both.countDown();await(both);synchronized(b){}}};
        Runnable right=()->{synchronized(b){both.countDown();await(both);synchronized(a){}}};
        Thread t1=new Thread(left,"deadlock-seat-0-to-1"),t2=new Thread(right,"deadlock-seat-1-to-0");
        t1.setDaemon(true);t2.setDaemon(true);t1.start();t2.start();Thread.sleep(200);
        ThreadMXBean bean=ManagementFactory.getThreadMXBean();long[] ids=bean.findDeadlockedThreads();
        if(ids==null)System.out.println("Deadlock not detected yet");
        else for(ThreadInfo info:bean.getThreadInfo(ids,true,true))System.out.println(info);
        System.out.println("Corrected approach: swapSafe() orders locks by seat id.");
    }
    private static void await(CountDownLatch latch){try{latch.await();}catch(InterruptedException e){Thread.currentThread().interrupt();}}
}
