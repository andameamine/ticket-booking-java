package com.concert;

interface BookingEngine extends AutoCloseable {
    BookingResult book(BookingRequest request);
    int soldCount();
    void shutdown();
    default void close() { shutdown(); }
}
