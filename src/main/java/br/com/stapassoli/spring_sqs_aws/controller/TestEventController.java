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
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

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
@Tag(name = "Testes de eventos", description = "Endpoints de simulação local do worker SQS (processamento síncrono, assíncrono, fila e DLQ).")
public class TestEventController {

    static final String EXAMPLE_EVENTBRIDGE_BANK_RETURN = """
            {
              "id": "IdHubdeEventos",
              "detail-type": "finance.invoice.bank-return",
              "source": "event-hub",
              "time": "2026-05-08T12:57:08Z",
              "datacontenttype": "application/json",
              "detail": {
                "invoiceId": "CHAVEPORTALNACIONAL",
                "payment": [
                  {
                    "key": "numeroSeqPG"
                  }
                ],
                "invoiceStatus": "Pago"
              }
            }
            """;

    static final String EXAMPLE_EVENTBRIDGE_COMPOSITE_KEY = """
            {
              "id": "IdHubdeEventos-composta",
              "detail-type": "finance.invoice.bank-return",
              "source": "event-hub",
              "time": "2026-05-08T12:57:08Z",
              "datacontenttype": "application/json",
              "detail": {
                "document": "12345678000199",
                "issueDate": "2026-05-08",
                "type": "NFS-e",
                "number": "1001",
                "serie": "1",
                "payment": [
                  {
                    "key": "numeroSeqPG"
                  }
                ]
              }
            }
            """;

    static final String EXAMPLE_SAMPLE_EVENT = """
            {
              "eventId": "evt-http-001",
              "eventType": "ORDER_CREATED",
              "source": "POSTMAN",
              "data": {
                "valor": 99.90
              }
            }
            """;

    private final EventProcessorService eventProcessorService;
    private final EventRepository eventRepository;
    private final ApplicationEventPublisher eventPublisher;
    private final ObjectMapper objectMapper;
    private final Optional<LocalMockDlqPublisher> localMockDlqPublisher;
    private final DlqPublisher dlqPublisher;
    private final QueueProducer queueProducer;

    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(
            summary = "Processar evento de forma síncrona",
            description = "Chama a mesma camada de serviço do consumidor SQS e devolve o evento persistido."
    )
    @io.swagger.v3.oas.annotations.parameters.RequestBody(
            required = true,
            description = "JSON do evento. Prefira o envelope EventBridge, com os dados de negócio em detail.",
            content = @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = EventMessageDTO.class),
                    examples = {
                            @ExampleObject(name = "Retorno bancário com invoiceId", value = EXAMPLE_EVENTBRIDGE_BANK_RETURN),
                            @ExampleObject(name = "Retorno bancário com chave composta", value = EXAMPLE_EVENTBRIDGE_COMPOSITE_KEY),
                            @ExampleObject(name = "Evento sample (legado de testes)", value = EXAMPLE_SAMPLE_EVENT)
                    }
            )
    )
    @ApiResponses({
            @ApiResponse(responseCode = "201", description = "Evento processado e persistido",
                    content = @Content(schema = @Schema(implementation = EventEntity.class))),
            @ApiResponse(responseCode = "400", description = "JSON inválido (poison pill encaminhado à DLQ)"),
            @ApiResponse(responseCode = "422", description = "Falha de negócio ou payload rejeitado")
    })
    public ResponseEntity<?> processarEventoDireto(@org.springframework.web.bind.annotation.RequestBody String rawPayload) {
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

    @PostMapping(value = "/assincrono", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(
            summary = "Publicar evento na fila simulada (assíncrono)",
            description = "Publica um Spring Event. O SimuladorFilaLocalListener processa em background com retentativas."
    )
    @io.swagger.v3.oas.annotations.parameters.RequestBody(
            required = true,
            content = @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    examples = {
                            @ExampleObject(name = "Retorno bancário com invoiceId", value = EXAMPLE_EVENTBRIDGE_BANK_RETURN),
                            @ExampleObject(name = "Evento sample", value = EXAMPLE_SAMPLE_EVENT)
                    }
            )
    )
    @ApiResponse(responseCode = "202", description = "Evento aceito na fila interna")
    public ResponseEntity<Map<String, Object>> processarEventoAssincrono(
            @org.springframework.web.bind.annotation.RequestBody String rawPayload
    ) {
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

    @PostMapping(value = "/fila", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(
            summary = "Publicar na fila via QueueProducer",
            description = "Envia a mensagem para o QueueProducer (AWS SQS ou simulador local, conforme a configuração)."
    )
    @io.swagger.v3.oas.annotations.parameters.RequestBody(
            required = true,
            content = @Content(
                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                    examples = @ExampleObject(name = "Retorno bancário com invoiceId", value = EXAMPLE_EVENTBRIDGE_BANK_RETURN)
            )
    )
    @ApiResponse(responseCode = "202", description = "Mensagem enviada para a fila")
    public ResponseEntity<Map<String, Object>> publicarNaFila(
            @org.springframework.web.bind.annotation.RequestBody String rawPayload
    ) {
        log.info("Recebida requisição para publicação direta na fila via QueueProducer: {}", rawPayload);
        queueProducer.send(rawPayload);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(Map.of(
                "status", "ACCEPTED",
                "message", "Mensagem enviada com sucesso para a fila normal via QueueProducer."
        ));
    }

    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(summary = "Listar eventos (teste)", description = "Lista todos os eventos persistidos. Para listagem paginada use GET /api/consultas/eventos.")
    @ApiResponse(responseCode = "200", description = "Lista de eventos")
    public ResponseEntity<List<EventEntity>> listarEventos() {
        return ResponseEntity.ok(eventRepository.findAll());
    }

    @GetMapping(value = "/{eventId}", produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(summary = "Buscar evento por eventId")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Evento encontrado",
                    content = @Content(schema = @Schema(implementation = EventEntity.class))),
            @ApiResponse(responseCode = "404", description = "Evento não encontrado")
    })
    public ResponseEntity<?> buscarPorEventId(
            @Parameter(description = "Identificador de negócio do evento (campo eventId / id do EventBridge).", example = "IdHubdeEventos")
            @PathVariable String eventId
    ) {
        return eventRepository.findByEventId(eventId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping(value = "/dlq", produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(summary = "Listar mensagens da DLQ local", description = "Consulta as mensagens capturadas na Dead Letter Queue simulada em memória.")
    @ApiResponse(responseCode = "200", description = "Mensagens da DLQ local")
    public ResponseEntity<List<DlqMessageRecord>> listarMensagensDlq() {
        if (localMockDlqPublisher.isPresent()) {
            return ResponseEntity.ok(localMockDlqPublisher.get().getCapturedMessages());
        }
        return ResponseEntity.ok(List.of());
    }
}
