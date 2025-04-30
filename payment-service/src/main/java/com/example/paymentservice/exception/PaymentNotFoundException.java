package com.example.paymentservice.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(HttpStatus.NOT_FOUND)
public class PaymentNotFoundException extends RuntimeException {
    public PaymentNotFoundException(Long orderId) {
        super("Payment not found for order ID: " + orderId);
    }
    public PaymentNotFoundException(String message) {
        super(message);
    }
}
