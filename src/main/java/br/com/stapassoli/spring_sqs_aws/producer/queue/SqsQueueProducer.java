package br.com.stapassoli.spring_sqs_aws.producer.queue;

import br.com.stapassoli.spring_sqs_aws.config.AwsSqsProperties;
import br.com.stapassoli.spring_sqs_aws.domain.dto.EventMessageDTO;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.awspring.cloud.sqs.operations.SqsTemplate;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * Implementação de produção para envio em filas normais do AWS SQS (ou LocalStack).
 * Ativada apenas quando app.sqs.enabled=true.
 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "app.sqs.enabled", havingValue = "true")
public class SqsQueueProducer implements QueueProducer {

    private final SqsTemplate sqsTemplate;
    private final AwsSqsProperties properties;
    private final ObjectMapper objectMapper;

    @Override
    public void send(EventMessageDTO event) {
        send(event, Map.of());
    }

    @Override
    public void send(EventMessageDTO event, Map<String, Object> headers) {
        try {
            String payload = objectMapper.writeValueAsString(event);
            sendToQueue(properties.getQueues().getEventsQueue(), payload, headers);
        } catch (JsonProcessingException e) {
            log.error("[AWS SQS Producer] Erro ao serializar evento para envio à fila: {}", event, e);
            throw new IllegalArgumentException("Falha na serialização do evento", e);
        }
    }

    @Override
    public void send(String rawPayload) {
        sendToQueue(properties.getQueues().getEventsQueue(), rawPayload, Map.of());
    }

    @Override
    public void send(String rawPayload, Map<String, Object> headers) {
        sendToQueue(properties.getQueues().getEventsQueue(), rawPayload, headers);
    }

    @Override
    public void sendToQueue(String queueName, String rawPayload, Map<String, Object> headers) {
        log.info("[AWS SQS Producer] Publicando mensagem na fila '{}'", queueName);

        var messageBuilder = MessageBuilder.withPayload(rawPayload)
                .setHeader("x-produced-at", Instant.now().toString())
                .setHeader("x-message-id", UUID.randomUUID().toString());

        if (headers != null) {
            headers.forEach((key, value) -> {
                if (value != null) {
                    messageBuilder.setHeader(key, value.toString());
                }
            });
        }

        sqsTemplate.send(queueName, messageBuilder.build());
        log.info("[AWS SQS Producer] Mensagem publicada com sucesso na fila '{}'", queueName);
    }
}
