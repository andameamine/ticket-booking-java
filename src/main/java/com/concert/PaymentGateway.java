package com.concert;
import java.util.concurrent.ThreadLocalRandom;
@FunctionalInterface
interface PaymentGateway {
    boolean charge(String customerId) throws InterruptedException;
    static PaymentGateway simulated() { return id -> { Thread.sleep(ThreadLocalRandom.current().nextInt(50,301)); return ThreadLocalRandom.current().nextInt(10)!=0; }; }
}
