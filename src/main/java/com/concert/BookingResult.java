package com.concert;

sealed interface BookingResult permits Confirmed, Rejected, Expired {}
