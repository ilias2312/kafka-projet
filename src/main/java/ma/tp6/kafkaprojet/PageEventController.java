package ma.tp6.kafkaprojet;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;
import java.util.Random;

@RestController
public class PageEventController {

    @Autowired
    private StreamBridge streamBridge;

    // Producer REST : http://localhost:8082/publish/P3
    @GetMapping("/publish/{name}")
    public PageEvent publish(@PathVariable String name) {
        PageEvent e = new PageEvent(name, Math.random() > 0.5 ? "U1" : "U2",
                new Date(), 100 + new Random().nextInt(9000));
        streamBridge.send("events-input", e);
        return e;
    }
}
