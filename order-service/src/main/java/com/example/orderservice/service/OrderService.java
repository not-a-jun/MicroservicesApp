package com.example.orderservice.service;

import com.example.common.event.OrderEvent;
import com.example.common.model.OrderStatus;
import com.example.orderservice.dto.OrderRequest;
import com.example.orderservice.exception.OrderNotFoundException;
import com.example.orderservice.model.Order;
import com.example.orderservice.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {
    private final OrderRepository orderRepository;
    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    @Transactional
    public Order createOrder(OrderRequest request) {
        Order order = new Order();
        order.setCustomerId(request.getCustomerId());
        order.setDescription(request.getDescription());
        order.setAmount(request.getAmount());
        order.setStatus(OrderStatus.PROCESSING);
        order.setCreatedAt(LocalDateTime.now());

        Order savedOrder = orderRepository.save(order);

        // Отправка события о создании заказа
        OrderEvent event = new OrderEvent(
                savedOrder.getId(),
                "ORDER_CREATED",
                savedOrder.getCustomerId(),
                savedOrder.getAmount()
        );

        kafkaTemplate.send("order-events", event);
        log.info("Sent ORDER_CREATED event for order {}", savedOrder.getId());

        return savedOrder;
    }

    @KafkaListener(topics = "payment-events", groupId = "order-group")
    @Transactional
    public void handlePaymentEvent(OrderEvent event) {
        log.info("Received payment event: {} for order {}", event.getEventType(), event.getOrderId());

        Order order = orderRepository.findById(event.getOrderId())
                .orElseThrow(() -> new OrderNotFoundException(event.getOrderId()));

        switch (event.getEventType()) {
            case "PAYMENT_APPROVED":
                order.setStatus(OrderStatus.PAYMENT_RECEIVED);
                orderRepository.save(order);

                // Отправка события для резервирования билета
                OrderEvent ticketEvent = new OrderEvent(
                        order.getId(),
                        "RESERVE_TICKET",
                        order.getCustomerId(),
                        order.getAmount()
                );
                kafkaTemplate.send("ticket-events", ticketEvent);
                log.info("Sent RESERVE_TICKET event for order {}", order.getId());
                break;

            case "PAYMENT_REJECTED":
                order.setStatus(OrderStatus.FAILED);
                orderRepository.save(order);
                log.warn("Payment rejected for order {}", order.getId());
                break;
        }
    }

    @KafkaListener(topics = "ticket-events", groupId = "order-group")
    @Transactional
    public void handleTicketEvent(OrderEvent event) {
        log.info("Received ticket event: {} for order {}", event.getEventType(), event.getOrderId());

        Order order = orderRepository.findById(event.getOrderId())
                .orElseThrow(() -> new OrderNotFoundException(event.getOrderId()));

        switch (event.getEventType()) {
            case "TICKET_RESERVED":
                order.setStatus(OrderStatus.COMPLETED);
                orderRepository.save(order);
                log.info("Order {} completed successfully", order.getId());
                break;

            case "TICKET_REJECTED":
                order.setStatus(OrderStatus.FAILED);
                orderRepository.save(order);

                // Инициируем компенсирующую транзакцию для возврата платежа
                OrderEvent refundEvent = new OrderEvent(
                        order.getId(),
                        "INITIATE_REFUND",
                        order.getCustomerId(),
                        order.getAmount()
                );
                kafkaTemplate.send("payment-events", refundEvent);
                log.warn("Ticket reservation failed for order {}, initiating refund", order.getId());
                break;
        }
    }

    @Transactional
    public Order cancelOrder(Long orderId) {
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new OrderNotFoundException(orderId));

        if (order.getStatus() == OrderStatus.COMPLETED) {
            // Если заказ уже выполнен, инициируем процесс отмены
            order.setStatus(OrderStatus.CANCELLATION_IN_PROGRESS);
            orderRepository.save(order);

            OrderEvent cancelEvent = new OrderEvent(
                    order.getId(),
                    "CANCEL_ORDER",
                    order.getCustomerId(),
                    order.getAmount()
            );

            kafkaTemplate.send("order-events", cancelEvent);
            log.info("Started cancellation process for order {}", order.getId());
        } else {
            order.setStatus(OrderStatus.CANCELLED);
            orderRepository.save(order);
            log.info("Order {} cancelled", order.getId());
        }

        return order;
    }

    public Order getOrder(Long id) {
        return orderRepository.findById(id)
                .orElseThrow(() -> new OrderNotFoundException(id));
    }
}