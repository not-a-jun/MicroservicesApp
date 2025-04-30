package com.example.paymentservice.controller;

import com.example.paymentservice.dto.PaymentDetailsResponse;
import com.example.paymentservice.dto.PaymentProcessRequest;
import com.example.paymentservice.dto.PaymentStatusResponse;
import com.example.paymentservice.dto.RefundRequest;
import com.example.paymentservice.service.PaymentService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/payments")
@RequiredArgsConstructor
@Tag(name = "Payment API", description = "Управление платежами и возвратами")
public class PaymentController {

    private final PaymentService paymentService;

    @GetMapping("/{orderId}")
    @Operation(
            summary = "Получить информацию о платеже",
            responses = {
                    @ApiResponse(responseCode = "200", description = "Платеж найден"),
                    @ApiResponse(responseCode = "404", description = "Платеж не найден")
            }
    )
    public ResponseEntity<PaymentDetailsResponse> getPaymentDetails(
            @Parameter(description = "ID заказа", required = true)
            @PathVariable Long orderId) {

        return ResponseEntity.ok(paymentService.getPayment(orderId));
    }

    @PostMapping("/process")
    @Operation(
            summary = "Обработать платеж",
            responses = {
                    @ApiResponse(responseCode = "202", description = "Платеж принят в обработку"),
                    @ApiResponse(responseCode = "400", description = "Неверные параметры запроса"),
                    @ApiResponse(responseCode = "409", description = "Конфликт статусов платежа")
            }
    )
    public ResponseEntity<Void> processPayment(
            @Parameter(description = "Данные платежа", required = true)
            @Valid @RequestBody PaymentProcessRequest request) {

        paymentService.processPayment(request);
        return ResponseEntity.accepted().build();
    }

    @PostMapping("/refund")
    @ResponseStatus(HttpStatus.ACCEPTED)
    @Operation(
            summary = "Инициировать возврат средств",
            responses = {
                    @ApiResponse(responseCode = "202", description = "Возврат принят в обработку"),
                    @ApiResponse(responseCode = "400", description = "Неверные параметры запроса"),
                    @ApiResponse(responseCode = "404", description = "Платеж не найден"),
                    @ApiResponse(responseCode = "409", description = "Невозможно выполнить возврат")
            }
    )
    public void initiateRefund(
            @Parameter(description = "Данные для возврата", required = true)
            @Valid @RequestBody RefundRequest request) {

        paymentService.initiateRefund(request);
    }

    @GetMapping("/status/{paymentId}")
    @Operation(
            summary = "Проверить статус платежа",
            responses = {
                    @ApiResponse(responseCode = "200", description = "Статус получен"),
                    @ApiResponse(responseCode = "404", description = "Платеж не найден")
            }
    )
    public ResponseEntity<PaymentStatusResponse> checkPaymentStatus(
            @Parameter(description = "ID платежа", required = true)
            @PathVariable Long paymentId) {

        return ResponseEntity.ok(paymentService.getPaymentStatus(paymentId));
    }
}