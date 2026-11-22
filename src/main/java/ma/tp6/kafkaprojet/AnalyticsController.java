package ma.tp6.kafkaprojet;

import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@RestController
public class AnalyticsController {

    @Autowired
    private InteractiveQueryService queryService;

    @GetMapping(path = "/analytics", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<Map<String, Long>> analytics() {
        return Flux.interval(Duration.ofSeconds(1)).map(seq -> {
            Map<String, Long> result = new HashMap<>();
            try {
                ReadOnlyWindowStore<String, Long> store =
                        queryService.getQueryableStore("page-count", QueryableStoreTypes.windowStore());
                Instant now = Instant.now();
                try (var it = store.fetchAll(now.minusSeconds(5), now)) {
                    while (it.hasNext()) {
                        var kv = it.next();
                        result.put(kv.key.key(), kv.value);
                    }
                }
            } catch (Exception ignored) {
                // le store n'est pas encore pret au demarrage
            }
            return result;
        });
    }
}
