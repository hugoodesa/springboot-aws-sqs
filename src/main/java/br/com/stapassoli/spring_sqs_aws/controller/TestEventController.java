package br.com.stapassoli.spring_sqs_aws.controller;

import br.com.stapassoli.spring_sqs_aws.consumer.local.EventoLocalMessage;
import br.com.stapassoli.spring_sqs_aws.domain.dto.EventMessageDTO;
import br.com.stapassoli.spring_sqs_aws.domain.entity.EventEntity;
import br.com.stapassoli.spring_sqs_aws.domain.repository.EventRepository;
import br.com.stapassoli.spring_sqs_aws.producer.dlq.DlqMessageRecord;
import br.com.stapassoli.spring_sqs_aws.producer.dlq.DlqPublisher;
import br.com.stapassoli.spring_sqs_aws.producer.dlq.LocalMockDlqPublisher;
import br.com.stapassoli.spring_sqs_aws.producer.queue.QueueProducer;
import br.com.stapassoli.spring_sqs_aws.service.EventProcessorService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Controller REST para teste e validação do fluxo de ponta a ponta em ambiente local.
 * Permite simular a ingestão de eventos via HTTP sem necessidade de conexão com AWS ou Docker.
 */
@Slf4j
@RestController
@RequestMapping("/api/test/eventos")
@RequiredArgsConstructor
public class TestEventController {

    private final EventProcessorService eventProcessorService;
    private final EventRepository eventRepository;
    private final ApplicationEventPublisher eventPublisher;
    private final ObjectMapper objectMapper;
    private final Optional<LocalMockDlqPublisher> localMockDlqPublisher;
    private final DlqPublisher dlqPublisher;
    private final QueueProducer queueProducer;

    /**
     * Processamento síncrono: chama diretamente a mesma camada de serviço que o SQS consome.
     * Ideal para validação imediata do retorno via Postman/cURL.
     */
    @PostMapping
    public ResponseEntity<?> processarEventoDireto(@RequestBody String rawPayload) {
        log.info("Recebida requisição síncrona de evento via HTTP POST: {}", rawPayload);

        EventMessageDTO dto;
        try {
            dto = objectMapper.readValue(rawPayload, EventMessageDTO.class);
        } catch (JsonProcessingException ex) {
            log.error("Payload HTTP inválido (Poison Pill): {}", rawPayload, ex);
            dlqPublisher.sendToDlq(rawPayload, "JSON_INVALIDO: " + ex.getOriginalMessage(), "HTTP-REQUEST", Map.of());
            eventProcessorService.markAsFailed(null, rawPayload, "POISON_PILL: " + ex.getMessage(), 1);
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of(
                    "status", "ERROR",
                    "error", "JSON_DESERIALIZATION_ERROR",
                    "message", "O JSON enviado é inválido e foi encaminhado para a DLQ.",
                    "details", ex.getOriginalMessage()
            ));
        }

        try {
            EventEntity savedEntity = eventProcessorService.processEvent(dto, rawPayload, 1);
            return ResponseEntity.status(HttpStatus.CREATED).body(savedEntity);
        } catch (Exception ex) {
            log.error("Erro durante o processamento do evento via controller: {}", ex.getMessage());
            return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY).body(Map.of(
                    "status", "ERROR",
                    "exception", ex.getClass().getSimpleName(),
                    "message", ex.getMessage()
            ));
        }
    }

    /**
     * Processamento assíncrono: publica um Spring Event simulando a fila SQS.
     * O SimuladorFilaLocalListener recebe e processa em background com retentativas simuladas.
     */
    @PostMapping("/assincrono")
    public ResponseEntity<Map<String, Object>> processarEventoAssincrono(@RequestBody String rawPayload) {
        log.info("Recebida requisição assíncrona de evento para fila simulada: {}", rawPayload);

        EventMessageDTO dto = null;
        try {
            dto = objectMapper.readValue(rawPayload, EventMessageDTO.class);
        } catch (Exception ignored) {
            // Se for inválido, o listener assíncrono detectará o poison pill
        }

        var localMessage = new EventoLocalMessage(rawPayload, dto, 1);
        eventPublisher.publishEvent(localMessage);

        return ResponseEntity.status(HttpStatus.ACCEPTED).body(Map.of(
                "status", "ACCEPTED",
                "message", "Evento postado com sucesso na fila interna assíncrona (simulador de SQS).",
                "simulatedMessageId", localMessage.messageId(),
                "publishedAt", localMessage.publishedAt().toString()
        ));
    }

    /**
     * Publicação em fila normal via QueueProducer (suporta tanto AWS SQS quanto simulador local).
     */
    @PostMapping("/fila")
    public ResponseEntity<Map<String, Object>> publicarNaFila(@RequestBody String rawPayload) {
        log.info("Recebida requisição para publicação direta na fila via QueueProducer: {}", rawPayload);
        queueProducer.send(rawPayload);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(Map.of(
                "status", "ACCEPTED",
                "message", "Mensagem enviada com sucesso para a fila normal via QueueProducer."
        ));
    }

    /**
     * Lista todos os eventos persistidos no banco de dados relacional H2.
     */
    @GetMapping
    public ResponseEntity<List<EventEntity>> listarEventos() {
        return ResponseEntity.ok(eventRepository.findAll());
    }

    /**
     * Consulta detalhes de um evento específico por seu eventId.
     */
    @GetMapping("/{eventId}")
    public ResponseEntity<?> buscarPorEventId(@PathVariable String eventId) {
        return eventRepository.findByEventId(eventId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    /**
     * Consulta as mensagens capturadas na Dead Letter Queue simulada em memória.
     */
    @GetMapping("/dlq")
    public ResponseEntity<List<DlqMessageRecord>> listarMensagensDlq() {
        if (localMockDlqPublisher.isPresent()) {
            return ResponseEntity.ok(localMockDlqPublisher.get().getCapturedMessages());
        }
        return ResponseEntity.ok(List.of());
    }
}
