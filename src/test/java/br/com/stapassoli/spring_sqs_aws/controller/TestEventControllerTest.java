package br.com.stapassoli.spring_sqs_aws.controller;

import br.com.stapassoli.spring_sqs_aws.domain.entity.EventEntity;
import br.com.stapassoli.spring_sqs_aws.domain.enums.EventStatus;
import br.com.stapassoli.spring_sqs_aws.domain.repository.EventRepository;
import br.com.stapassoli.spring_sqs_aws.producer.dlq.LocalMockDlqPublisher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
class TestEventControllerTest {

    @Autowired
    private WebApplicationContext context;

    @Autowired
    private EventRepository eventRepository;

    @Autowired
    private LocalMockDlqPublisher localDlqPublisher;

    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        this.mockMvc = MockMvcBuilders.webAppContextSetup(context).build();
        eventRepository.deleteAll();
        localDlqPublisher.clear();
    }

    @Test
    @DisplayName("POST /api/test/eventos: Deve processar síncronamente e persistir no banco H2")
    void shouldProcessSynchronouslyAndPersistInH2() throws Exception {
        String payload = """
                {
                    "eventId": "evt-http-001",
                    "eventType": "ORDER_CREATED",
                    "source": "POSTMAN",
                    "data": {
                        "valor": 99.90
                    }
                }
                """;

        mockMvc.perform(post("/api/test/eventos")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(payload))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.eventId").value("evt-http-001"))
                .andExpect(jsonPath("$.status").value("PROCESSED"));

        EventEntity entity = eventRepository.findByEventId("evt-http-001").orElseThrow();
        assertThat(entity.getStatus()).isEqualTo(EventStatus.PROCESSED);
        assertThat(entity.getPayload()).contains("evt-http-001");
    }

    @Test
    @DisplayName("POST /api/test/eventos: Deve capturar Poison Pill e enviar para a DLQ simulada")
    void shouldCapturePoisonPillAndSendToLocalDlq() throws Exception {
        String corruptPayload = "{ invalid-json ";

        mockMvc.perform(post("/api/test/eventos")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(corruptPayload))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("JSON_DESERIALIZATION_ERROR"));

        assertThat(localDlqPublisher.getCapturedMessages()).hasSize(1);
        assertThat(localDlqPublisher.getCapturedMessages().get(0).reason()).contains("JSON_INVALIDO");
    }

    @Test
    @DisplayName("POST /api/test/eventos/assincrono: Deve aceitar requisição (202) e processar em background via Spring Events")
    void shouldProcessAsynchronouslyViaSpringEvents() throws Exception {
        String payload = """
                {
                    "eventId": "evt-async-002",
                    "eventType": "USER_REGISTERED",
                    "source": "MOBILE_APP",
                    "data": {
                        "userId": "usr-888"
                    }
                }
                """;

        mockMvc.perform(post("/api/test/eventos/assincrono")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(payload))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.status").value("ACCEPTED"))
                .andExpect(jsonPath("$.simulatedMessageId").exists());

        // Aguarda a conclusão assíncrona do listener
        await().atMost(3, TimeUnit.SECONDS).untilAsserted(() -> {
            var opt = eventRepository.findByEventId("evt-async-002");
            assertThat(opt).isPresent();
            assertThat(opt.get().getStatus()).isEqualTo(EventStatus.PROCESSED);
        });
    }

    @Test
    @DisplayName("GET /api/test/eventos: Deve listar todos os eventos cadastrados")
    void shouldListEvents() throws Exception {
        eventRepository.save(EventEntity.builder()
                .eventId("evt-list-1")
                .eventType("TEST")
                .source("TEST")
                .payload("{}")
                .status(EventStatus.PROCESSED)
                .retryCount(0)
                .build());

        mockMvc.perform(get("/api/test/eventos"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].eventId").value("evt-list-1"));
    }

    @Test
    @DisplayName("GET /api/test/eventos/dlq: Deve listar mensagens na DLQ local")
    void shouldListDlqMessages() throws Exception {
        localDlqPublisher.sendToDlq("{\"erro\":\"simulado\"}", "MOTIVO_TESTE", "msg-001", null);

        mockMvc.perform(get("/api/test/eventos/dlq"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].reason").value("MOTIVO_TESTE"))
                .andExpect(jsonPath("$[0].originalMessageId").value("msg-001"));
    }

    @Test
    @DisplayName("POST /api/test/eventos/fila: Deve publicar evento na fila normal através do QueueProducer")
    void shouldPublishEventToQueueViaQueueProducer() throws Exception {
        String payload = """
                {
                    "eventId": "evt-producer-003",
                    "eventType": "PAYMENT_CONFIRMED",
                    "source": "PAYMENT_GATEWAY",
                    "data": {
                        "amount": 250.00
                    }
                }
                """;

        mockMvc.perform(post("/api/test/eventos/fila")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(payload))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.status").value("ACCEPTED"))
                .andExpect(jsonPath("$.message").value("Mensagem enviada com sucesso para a fila normal via QueueProducer."));

        // O QueueProducer local dispara o listener simulado e processa no banco H2
        await().atMost(3, TimeUnit.SECONDS).untilAsserted(() -> {
            var opt = eventRepository.findByEventId("evt-producer-003");
            assertThat(opt).isPresent();
            assertThat(opt.get().getStatus()).isEqualTo(EventStatus.PROCESSED);
        });
    }
}

