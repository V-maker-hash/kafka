package com.kafka.tutorial2;

import java.util.Queue;
import java.util.Random;

import static com.kafka.tutorial2.TwitterProducer.LINKED_BLOCKING_QUEUE;

public class ProducerUtil {
    private ProducerUtil() {
    }

    public static void fillQueue(Queue<String> queue) throws InterruptedException {
        int counter = 0;
        while (++counter <= 1000) {
            LINKED_BLOCKING_QUEUE.add("HELLO " + counter);
        }
    }
}
