package br.com.stapassoli.spring_sqs_aws.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Getter
@Setter
@Configuration
@ConfigurationProperties(prefix = "app.sqs")
public class AwsSqsProperties {

    private Queues queues = new Queues();
    private Listener listener = new Listener();

    @Getter
    @Setter
    public static class Queues {
        private String eventsQueue = "events-hub-queue";
        private String eventsDlq = "events-hub-dlq";
    }

    @Getter
    @Setter
    public static class Listener {
        private int maxConcurrentMessages = 10;
        private int maxMessagesPerPoll = 10;
        private int pollTimeoutSeconds = 10;
    }
}
