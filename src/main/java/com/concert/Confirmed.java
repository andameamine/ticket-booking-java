package com.concert;

record Confirmed(int seatId, String customerId) implements BookingResult {}
