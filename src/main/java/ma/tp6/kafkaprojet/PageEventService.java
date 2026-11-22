package ma.tp6.kafkaprojet;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Date;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@Service
public class PageEventService {

    // Supplier : genere un evenement chaque seconde -> events-input
    @Bean
    public Supplier<PageEvent> eventSupplier() {
        return () -> new PageEvent(
                Math.random() > 0.5 ? "P1" : "P2",
                Math.random() > 0.5 ? "U1" : "U2",
                new Date(),
                new Random().nextInt(9000));
    }

    // Consumer : lit events-input et affiche
    @Bean
    public Consumer<PageEvent> eventConsumer() {
        return e -> System.out.println("Consumed: " + e);
    }

    // Kafka Streams : compte les pages vues par fenetre de 5 s
    @Bean
    public Function<KStream<String, PageEvent>, KStream<String, Long>> analyticsProcessor() {
        return input -> input
                .filter((k, v) -> v.getDuration() > 100)
                .map((k, v) -> new KeyValue<>(v.getName(), v))
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(PageEvent.class)))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(5)))
                .count(Materialized.as("page-count"))
                .toStream()
                .map((k, v) -> new KeyValue<>(k.key(), v));
    }
}
