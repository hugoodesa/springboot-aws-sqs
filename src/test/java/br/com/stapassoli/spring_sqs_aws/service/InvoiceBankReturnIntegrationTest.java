package br.com.stapassoli.spring_sqs_aws.service;

import br.com.stapassoli.spring_sqs_aws.domain.dto.EventMessageDTO;
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

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class InvoiceBankReturnIntegrationTest {

    @Autowired
    private EventProcessorService eventProcessorService;

    @Autowired
    private InvoiceRepository invoiceRepository;

    @Autowired
    private EventRepository eventRepository;

    @BeforeEach
    void setUp() {
        eventRepository.deleteAll();
        invoiceRepository.deleteAll();
    }

    @Test
    @DisplayName("Integração: EventBridge com invoiceId atualiza a nota correta para Pago no H2")
    void shouldPersistPaidStatusForInvoiceIdentifiedByInvoiceId() {
        InvoiceEntity invoice = invoiceRepository.save(InvoiceEntity.builder()
                .invoiceId("CHAVEPORTALNACIONAL")
                .document("12345678000199")
                .issueDate("2026-05-08")
                .type("NFS-e")
                .number("1001")
                .serie("1")
                .status(InvoiceStatus.PENDENTE.getDescription())
                .createdAt(Instant.now())
                .build());

        Map<String, Object> detail = new HashMap<>();
        detail.put("invoiceId", "CHAVEPORTALNACIONAL");
        detail.put("payment", List.of(Map.of("key", "numeroSeqPG")));
        detail.put("unusedAttribute", "ok");

        EventMessageDTO event = new EventMessageDTO(
                "IdHubdeEventos",
                "finance.invoice.bank-return",
                "event-hub",
                Instant.parse("2026-05-08T12:57:08Z"),
                null,
                detail
        );

        EventEntity processed = eventProcessorService.processEvent(event, "{}", 1);
        InvoiceEntity updated = invoiceRepository.findById(invoice.getId()).orElseThrow();

        assertThat(processed.getStatus()).isEqualTo(EventStatus.PROCESSED);
        assertThat(updated.getStatus()).isEqualTo(InvoiceStatus.PAGO.getDescription());
        assertThat(updated.getLastPaymentKey()).isEqualTo("numeroSeqPG");
        assertThat(invoiceRepository.count()).isEqualTo(1);
    }

    @Test
    @DisplayName("Integração: reprocessamento do mesmo eventId não altera novamente a nota")
    void shouldNotChangeInvoiceAgainOnEventReplay() {
        InvoiceEntity invoice = invoiceRepository.save(InvoiceEntity.builder()
                .invoiceId("INV-REPLAY")
                .status(InvoiceStatus.PENDENTE.getDescription())
                .createdAt(Instant.now())
                .build());

        Map<String, Object> detail = new HashMap<>();
        detail.put("invoiceId", "INV-REPLAY");
        detail.put("payment", List.of(Map.of("key", "pg-1")));
        EventMessageDTO event = new EventMessageDTO(
                "evt-replay",
                "finance.invoice.bank-return",
                "event-hub",
                Instant.now(),
                null,
                detail
        );

        eventProcessorService.processEvent(event, "{}", 1);
        Instant updatedAtAfterFirst = invoiceRepository.findById(invoice.getId()).orElseThrow().getUpdatedAt();

        EventEntity second = eventProcessorService.processEvent(event, "{}", 2);
        InvoiceEntity afterReplay = invoiceRepository.findById(invoice.getId()).orElseThrow();

        assertThat(second.getStatus()).isEqualTo(EventStatus.PROCESSED);
        assertThat(afterReplay.getStatus()).isEqualTo(InvoiceStatus.PAGO.getDescription());
        assertThat(afterReplay.getUpdatedAt()).isEqualTo(updatedAtAfterFirst);
        assertThat(eventRepository.count()).isEqualTo(1);
    }
}
