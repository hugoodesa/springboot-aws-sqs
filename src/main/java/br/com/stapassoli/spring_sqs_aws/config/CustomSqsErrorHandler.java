package br.com.stapassoli.spring_sqs_aws.config;

import br.com.stapassoli.spring_sqs_aws.exception.NonRetryableException;
import br.com.stapassoli.spring_sqs_aws.producer.dlq.DlqPublisher;
import io.awspring.cloud.sqs.listener.errorhandler.ErrorHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.messaging.Message;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Interceptador global de falhas no container @SqsListener do Spring Cloud AWS 3.x.
 * Ativado apenas quando app.sqs.enabled=true.
 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "app.sqs.enabled", havingValue = "true")
public class CustomSqsErrorHandler implements ErrorHandler<Object> {

    private final DlqPublisher dlqPublisher;

    @Override
    public void handle(Message<Object> message, Throwable t) {
        String payloadStr = message.getPayload() != null ? message.getPayload().toString() : "";
        String messageId = message.getHeaders().getId() != null ? message.getHeaders().getId().toString() : "UNKNOWN";

        log.error("Erro capturado no container SQS para MessageId '{}'. Tipo da exceção: {}",
                messageId, t.getClass().getSimpleName(), t);

        Throwable rootCause = getRootCause(t);

        if (isNonRetryable(rootCause)) {
            log.warn("Detectado erro fatal/não recuperável (Poison Pill). Encaminhando diretamente para a DLQ.");
            dlqPublisher.sendToDlq(
                    payloadStr,
                    rootCause.getMessage(),
                    messageId,
                    Map.of(
                            "x-exception-class", rootCause.getClass().getName(),
                            "x-poison-pill", "true"
                    )
            );
            // Ao não relançar a exceção, o container reconhece a mensagem como tratada e a remove da fila principal
            return;
        }

        log.warn("Falha transitória detectada. Propagando exceção para acionar Visibility Timeout e Redrive Policy da AWS SQS.");
        if (t instanceof RuntimeException runtimeException) {
            throw runtimeException;
        }
        throw new RuntimeException("Falha na execução do listener SQS", t);
    }

    private boolean isNonRetryable(Throwable t) {
        return t instanceof NonRetryableException
                || t instanceof MessageConversionException
                || t instanceof com.fasterxml.jackson.core.JsonProcessingException;
    }

    private Throwable getRootCause(Throwable throwable) {
        Throwable root = throwable;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        return root;
    }
}
