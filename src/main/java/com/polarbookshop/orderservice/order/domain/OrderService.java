package com.polarbookshop.orderservice.order.domain;

import com.polarbookshop.orderservice.book.Book;
import com.polarbookshop.orderservice.book.BookClient;
import com.polarbookshop.orderservice.order.event.OrderAcceptedMessage;
import com.polarbookshop.orderservice.order.event.OrderDisptchedMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class OrderService {
    private static final Logger log=
            LoggerFactory.getLogger(OrderService.class);
    private final OrderRepository orderRepository;
    private final BookClient bookClient;
    private final StreamBridge streamBridge;

    public OrderService(OrderRepository orderRepository, BookClient bookClient, StreamBridge streamBridge) {
        this.orderRepository = orderRepository;
        this.bookClient = bookClient;
        this.streamBridge = streamBridge;
    }

    public  Flux<Order> consumeOrderDispatchedEvent(Flux<OrderDisptchedMessage> flux) {
        return flux.flatMap(message-> orderRepository.findById(message.orderId()))
                .map(this::buildDispatchedOrder)
                .flatMap(orderRepository::save);
    }

    private Order buildDispatchedOrder(Order existingOrder) {
        return new Order(
                existingOrder.id(),
                existingOrder.bookIsbn(),
                existingOrder.bookName(),
                existingOrder.bookPrice(),
                existingOrder.quantity(),
                OrderStatus.DISPATCHED,
                existingOrder.createdDate(),
                existingOrder.lastModifiedDate(),
                existingOrder.createdBy(),
                existingOrder.lastModifiedBy(),
                existingOrder.version());
    }

    public Flux<Order> getAllOrders(String userId){
        return orderRepository.findAllByCreatedBy(userId);
    }
    @Transactional
    public Mono<Order> submitOrder(String isbn, int quantity){
        return  bookClient.getBookByIsbn(isbn).map(book->buildAcceptedOrder(book,quantity))
                        .defaultIfEmpty(buildRejectedOrder(isbn,quantity))
                .flatMap(orderRepository::save)
                .doOnNext(this::publishOrderAcceptedEvent);
    }

    private Order buildAcceptedOrder(Book book, int quantity) {
        return Order.of(book.isbn(),book.title()+"-"+book.author(),book.price(),quantity,OrderStatus.ACCEPTED);
    }

    public static Order buildRejectedOrder(String isbn, int quantity) {
        return Order.of(isbn, null, null, quantity, OrderStatus.REJECTED);
    }
    private void publishOrderAcceptedEvent(Order order){
        if(!order.status().equals(OrderStatus.ACCEPTED))
            return;
        var orderAcceptedMessage=new OrderAcceptedMessage(order.id());
        log.info("Sending order accepted event with id {}",orderAcceptedMessage.orderId());
        var result=streamBridge.send("acceptOrder-out-0",orderAcceptedMessage);
        log.info("Result of sending order accepted event with id {}: {}",order.id(),result);
    }
}
