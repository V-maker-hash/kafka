package com.kafka.tutorial2;

import com.fasterxml.jackson.core.JsonFactory;
import org.json.JSONObject;

import java.util.Queue;

import static com.kafka.tutorial2.TwitterProducer.LINKED_BLOCKING_QUEUE;

public class ProducerUtil {
    private ProducerUtil() {
    }

    public static void fillQueue(Queue<String> queue) throws InterruptedException {
        int counter = 0;
        while (counter++ <= 1) {
            String jsonString = new JSONObject()
                    .put("JSON1", "Hello World!")
                    .put("JSON2", "Hello my World!")
                    .put("JSON3", new JSONObject().put("key1", "value1"))
                    .toString();
            LINKED_BLOCKING_QUEUE.add(jsonString);
        }
    }
}
