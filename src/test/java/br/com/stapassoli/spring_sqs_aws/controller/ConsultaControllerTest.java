package br.com.stapassoli.spring_sqs_aws.controller;

import br.com.stapassoli.spring_sqs_aws.domain.entity.EventEntity;
import br.com.stapassoli.spring_sqs_aws.domain.entity.InvoiceEntity;
import br.com.stapassoli.spring_sqs_aws.domain.enums.EventStatus;
import br.com.stapassoli.spring_sqs_aws.domain.enums.InvoiceStatus;
import br.com.stapassoli.spring_sqs_aws.domain.repository.EventRepository;
import br.com.stapassoli.spring_sqs_aws.domain.repository.InvoiceRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import java.time.Instant;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
class ConsultaControllerTest {

    @Autowired
    private WebApplicationContext context;

    @Autowired
    private EventRepository eventRepository;

    @Autowired
    private InvoiceRepository invoiceRepository;

    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        this.mockMvc = MockMvcBuilders.webAppContextSetup(context).build();
        eventRepository.deleteAll();
        invoiceRepository.deleteAll();

        eventRepository.save(event("evt-1", Instant.parse("2026-05-01T10:00:00Z")));
        eventRepository.save(event("evt-2", Instant.parse("2026-05-02T10:00:00Z")));
        eventRepository.save(event("evt-3", Instant.parse("2026-05-03T10:00:00Z")));

        invoiceRepository.save(invoice("INV-1"));
        invoiceRepository.save(invoice("INV-2"));
    }

    @Test
    @DisplayName("GET /api/consultas/eventos sem paginação deve retornar todos os registros")
    void shouldReturnAllEventsWhenPaginationIsOmitted() throws Exception {
        mockMvc.perform(get("/api/consultas/eventos"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content.length()").value(3))
                .andExpect(jsonPath("$.totalElements").value(3));
    }

    @Test
    @DisplayName("GET /api/consultas/eventos com page e size deve paginar")
    void shouldPaginateEventsWhenPageAndSizeAreProvided() throws Exception {
        mockMvc.perform(get("/api/consultas/eventos")
                        .param("page", "0")
                        .param("size", "2")
                        .param("sort", "eventId")
                        .param("direction", "ASC"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content.length()").value(2))
                .andExpect(jsonPath("$.totalElements").value(3))
                .andExpect(jsonPath("$.content[0].eventId").value("evt-1"));
    }

    @Test
    @DisplayName("GET /api/consultas/notas-fiscais sem paginação deve retornar todos os registros")
    void shouldReturnAllInvoicesWhenPaginationIsOmitted() throws Exception {
        mockMvc.perform(get("/api/consultas/notas-fiscais"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content.length()").value(2))
                .andExpect(jsonPath("$.totalElements").value(2));
    }

    @Test
    @DisplayName("GET /v3/api-docs deve expor a especificação OpenAPI")
    void shouldExposeOpenApiSpec() throws Exception {
        mockMvc.perform(get("/v3/api-docs"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.info.title").value("cor-invoice-bank-return-wrk"))
                .andExpect(jsonPath("$.paths['/api/test/eventos']").exists())
                .andExpect(jsonPath("$.paths['/api/consultas/eventos']").exists())
                .andExpect(jsonPath("$.paths['/api/consultas/notas-fiscais']").exists());
    }

    private EventEntity event(String eventId, Instant createdAt) {
        return EventEntity.builder()
                .eventId(eventId)
                .eventType("finance.invoice.bank-return")
                .source("TEST")
                .payload("{}")
                .status(EventStatus.PROCESSED)
                .retryCount(0)
                .createdAt(createdAt)
                .build();
    }

    private InvoiceEntity invoice(String invoiceId) {
        return InvoiceEntity.builder()
                .invoiceId(invoiceId)
                .status(InvoiceStatus.PENDENTE.getDescription())
                .createdAt(Instant.now())
                .build();
    }
}
