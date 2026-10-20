package br.com.stapassoli.spring_sqs_aws.consumer;

import br.com.stapassoli.spring_sqs_aws.domain.dto.EventMessageDTO;
import br.com.stapassoli.spring_sqs_aws.exception.NonRetryableException;
import br.com.stapassoli.spring_sqs_aws.producer.dlq.DlqPublisher;
import br.com.stapassoli.spring_sqs_aws.service.EventProcessorService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.awspring.cloud.sqs.annotation.SqsListener;
import io.awspring.cloud.sqs.listener.SqsHeaders;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Consumidor de produção do Hub de Eventos AWS SQS.
 * Ativado apenas quando app.sqs.enabled=true.
 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "app.sqs.enabled", havingValue = "true")
public class EventConsumer {

    private final EventProcessorService eventProcessorService;
    private final DlqPublisher dlqPublisher;
    private final ObjectMapper objectMapper;

    @SqsListener("${app.sqs.queues.events-queue}")
    public void consume(
            @Payload String rawPayload,
            @Header(name = SqsHeaders.MessageSystemAttributes.SQS_APPROXIMATE_RECEIVE_COUNT, defaultValue = "1") String receiveCountStr,
            @Header(name = "id", required = false) String springMessageId,
            @Header(name = SqsHeaders.SQS_RECEIPT_HANDLE_HEADER, required = false) String receiptHandle
    ) {
        int receiveCount = parseReceiveCount(receiveCountStr);
        log.info("Mensagem recebida da fila SQS [MessageId='{}', ApproximateReceiveCount={}]", springMessageId, receiveCount);

        EventMessageDTO eventDTO;
        try {
            eventDTO = objectMapper.readValue(rawPayload, EventMessageDTO.class);
        } catch (JsonProcessingException ex) {
            log.error("Erro crítico de desserialização (Poison Pill). Conteúdo incompatível com EventMessageDTO: '{}'", rawPayload, ex);
            dlqPublisher.sendToDlq(
                    rawPayload,
                    "JSON_DESERIALIZATION_ERROR: " + ex.getOriginalMessage(),
                    springMessageId,
                    Map.of(
                            "x-approximate-receive-count", String.valueOf(receiveCount),
                            "x-exception-class", ex.getClass().getName(),
                            "x-poison-pill", "true"
                    )
            );
            eventProcessorService.markAsFailed(null, rawPayload, "POISON_PILL: " + ex.getMessage(), receiveCount);
            // Retorna sem lançar exceção para que o container confirme o recebimento (ACK) e descarte da fila principal
            return;
        }

        try {
            // Executa processamento de negócio e persistência via JPA
            eventProcessorService.processEvent(eventDTO, rawPayload, receiveCount);
            log.info("Evento processado com sucesso [eventId='{}']", eventDTO.eventId());

        } catch (NonRetryableException ex) {
            log.error("Erro fatal/não recuperável de negócio para eventId='{}'. Despachando diretamente para DLQ.", eventDTO.eventId(), ex);
            dlqPublisher.sendToDlq(
                    rawPayload,
                    "NON_RETRYABLE_ERROR: " + ex.getMessage(),
                    springMessageId,
                    Map.of(
                            "x-event-id", eventDTO.eventId(),
                            "x-event-type", eventDTO.eventType() != null ? eventDTO.eventType() : "UNKNOWN",
                            "x-approximate-receive-count", String.valueOf(receiveCount),
                            "x-exception-class", ex.getClass().getName()
                    )
            );
            eventProcessorService.markAsFailed(eventDTO.eventId(), rawPayload, ex.getMessage(), receiveCount);
            // Mensagem tratada e descartada da fila principal
            return;

        } catch (Exception ex) {
            log.warn("Falha recuperável ao processar evento [eventId='{}', tentativa={}]. Relançando exceção para acionar Visibility Timeout / Redrive Policy nativa da AWS SQS. Causa: {}",
                    eventDTO.eventId(), receiveCount, ex.getMessage());
            // Relança a exceção para que o container SQS NÃO envie o ACK, permitindo o fluxo de retentativa nativo da AWS
            throw ex;
        }
    }

    private int parseReceiveCount(String receiveCountStr) {
        try {
            return Integer.parseInt(receiveCountStr);
        } catch (NumberFormatException e) {
            return 1;
        }
    }
}
