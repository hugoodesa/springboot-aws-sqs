package br.com.stapassoli.spring_sqs_aws.producer.dlq;

import br.com.stapassoli.spring_sqs_aws.config.AwsSqsProperties;
import io.awspring.cloud.sqs.operations.SqsTemplate;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Map;

/**
 * Implementação de DLQ para produção/AWS real ou LocalStack via SQS.
 * Ativada apenas quando app.sqs.enabled=true.
 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "app.sqs.enabled", havingValue = "true")
public class EventDlqPublisher implements DlqPublisher {

    private final SqsTemplate sqsTemplate;
    private final AwsSqsProperties properties;


    @Override
    public void sendToDlq(String originalPayload, String reason, String originalMessageId, Map<String, Object> additionalHeaders) {
        String dlqQueueName = properties.getQueues().getEventsDlq();
        log.warn("[AWS SQS DLQ] Encaminhando mensagem para a DLQ '{}'. Motivo: '{}', MessageId original: '{}'",
                dlqQueueName, reason, originalMessageId);

        var messageBuilder = MessageBuilder.withPayload(originalPayload)
                .setHeader("x-failure-reason", reason)
                .setHeader("x-failed-at", Instant.now().toString())
                .setHeader("x-original-queue", properties.getQueues().getEventsQueue());

        if (originalMessageId != null) {
            messageBuilder.setHeader("x-original-message-id", originalMessageId);
        }

        if (additionalHeaders != null) {
            additionalHeaders.forEach((key, value) -> {
                if (value != null) {
                    messageBuilder.setHeader(key, value.toString());
                }
            });
        }

        sqsTemplate.send(dlqQueueName, messageBuilder.build());
        log.info("[AWS SQS DLQ] Mensagem enviada com sucesso para a DLQ '{}'", dlqQueueName);
    }
}
