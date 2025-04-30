package com.example.paymentservice.external;

import java.math.BigDecimal;

public interface ExternalPaymentGateway {
    boolean processPayment(String customerId, BigDecimal amount);
    boolean processRefund(String customerId, BigDecimal amount);
}
