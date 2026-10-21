package br.com.stapassoli.spring_sqs_aws.service;

import br.com.stapassoli.spring_sqs_aws.domain.dto.EventMessageDTO;
import br.com.stapassoli.spring_sqs_aws.domain.entity.EventEntity;
import br.com.stapassoli.spring_sqs_aws.domain.enums.EventStatus;
import br.com.stapassoli.spring_sqs_aws.domain.repository.EventRepository;
import br.com.stapassoli.spring_sqs_aws.exception.BusinessException;
import br.com.stapassoli.spring_sqs_aws.exception.NonRetryableException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class EventProcessorService {

    private static final String BANK_RETURN_EVENT_TYPE = "finance.invoice.bank-return";

    private final EventRepository eventRepository;
    private final InvoiceService invoiceService;

    /**
     * Processa o evento com garantia de idempotência e persistência transacional.
     *
     * @param eventDTO DTO desserializado do evento
     * @param rawPayload Conteúdo JSON original recebido da fila
     * @param receiveCount Contador de tentativas de entrega fornecido pelo SQS
     * @return Entidade persistida com status atualizado
     */
    @Transactional
    public EventEntity processEvent(EventMessageDTO eventDTO, String rawPayload, int receiveCount) {
        if (eventDTO == null) {
            throw new NonRetryableException("Payload inválido: o conteúdo da mensagem não pode ser nulo.");
        }

        log.info("Iniciando processamento do evento [eventId='{}', eventType='{}', tentativa={}]",
                eventDTO.eventId(), eventDTO.eventType(), receiveCount);

        // 1. Validações de integridade e negócio (Poison Pills geram NonRetryableException)
        if (eventDTO.eventId() == null || eventDTO.eventId().isBlank()) {
            throw new NonRetryableException("Payload inválido: o campo 'eventId' é obrigatório.");
        }
        if (eventDTO.eventType() == null || eventDTO.eventType().isBlank()) {
            throw new NonRetryableException("Payload inválido: o campo 'eventType' é obrigatório.");
        }

        // 2. Garantia de Idempotência: verificar se evento já foi processado
        Optional<EventEntity> existingEventOpt = eventRepository.findByEventId(eventDTO.eventId());
        if (existingEventOpt.isPresent()) {
            EventEntity existing = existingEventOpt.get();
            if (existing.getStatus() == EventStatus.PROCESSED) {
                log.warn("Evento duplicado detectado! [eventId='{}'] já foi processado com sucesso em {}. Ignorando reprocessamento.",
                        eventDTO.eventId(), existing.getProcessedAt());
                return existing;
            }
            log.info("Evento [eventId='{}'] já registrado com status '{}'. Atualizando tentativa para {}.",
                    eventDTO.eventId(), existing.getStatus(), receiveCount);
            existing.setRetryCount(receiveCount);
            return executeBusinessLogic(existing, eventDTO);
        }

        // 3. Criação da nova entidade em estado PENDING
        EventEntity newEntity = EventEntity.builder()
                .eventId(eventDTO.eventId())
                .eventType(eventDTO.eventType())
                .source(eventDTO.source() != null ? eventDTO.source() : "UNKNOWN")
                .payload(rawPayload)
                .status(EventStatus.PENDING)
                .retryCount(receiveCount)
                .createdAt(Instant.now())
                .build();

        return executeBusinessLogic(newEntity, eventDTO);
    }

    private EventEntity executeBusinessLogic(EventEntity entity, EventMessageDTO eventDTO) {
        // Exemplo: Simulação controlada de erros para validação do comportamento com DLQ
        if ("SIMULATE_NON_RETRYABLE_ERROR".equalsIgnoreCase(eventDTO.eventType())) {
            throw new NonRetryableException("Falha fatal e irreversível de negócio: dados não processáveis.");
        }

        if ("SIMULATE_RETRYABLE_ERROR".equalsIgnoreCase(eventDTO.eventType()) && entity.getRetryCount() < 3) {
            throw new BusinessException("Falha transitória simulada (ex: banco temporariamente instável). Tentativa: " + entity.getRetryCount());
        }

        if (isBankReturnEvent(eventDTO.eventType())) {
            invoiceService.updatePaymentStatusToPaid(eventDTO);
        }

        // 4. Conclusão do processamento e persistência
        entity.setStatus(EventStatus.PROCESSED);
        entity.setProcessedAt(Instant.now());
        entity.setErrorMessage(null);

        EventEntity saved = eventRepository.save(entity);
        log.info("Evento persistido com sucesso no banco de dados [id={}, eventId='{}', status={}]",
                saved.getId(), saved.getEventId(), saved.getStatus());

        return saved;
    }

    private boolean isBankReturnEvent(String eventType) {
        if (eventType == null || eventType.isBlank()) {
            return false;
        }

        String normalized = eventType
                .toLowerCase()
                .replace("_", ".")
                .replace("-", ".")
                .replace(" ", "")
                .replace(".", "");

        return normalized.contains("financeinvoice")
                || normalized.contains("invoicebankreturn")
                || normalized.contains("invoice.return")
                || normalized.contains("invoicebank")
                || BANK_RETURN_EVENT_TYPE.equalsIgnoreCase(eventType)
                || eventType.toLowerCase().contains("invoice.bank.return")
                || eventType.toLowerCase().contains("finance.invoice");
    }

    /**
     * Registra o evento com status de falha quando for descartado ou encaminhado para a DLQ.
     */
    @Transactional
    public void markAsFailed(String eventId, String rawPayload, String errorMessage, int retryCount) {
        try {
            EventEntity entity = eventRepository.findByEventId(eventId)
                    .orElseGet(() -> EventEntity.builder()
                            .eventId(eventId != null ? eventId : "UNKNOWN-" + Instant.now().toEpochMilli())
                            .eventType("UNKNOWN")
                            .source("SQS_CONSUMER")
                            .payload(rawPayload)
                            .createdAt(Instant.now())
                            .build()
                    );

            entity.setStatus(EventStatus.SENT_TO_DLQ);
            entity.setErrorMessage(errorMessage);
            entity.setRetryCount(retryCount);
            entity.setProcessedAt(Instant.now());
            eventRepository.save(entity);
            log.info("Registro de evento atualizado para status SENT_TO_DLQ [eventId='{}']", eventId);
        } catch (Exception ex) {
            log.error("Falha ao registrar status de erro no banco de dados para eventId='{}': {}", eventId, ex.getMessage(), ex);
        }
    }
}
