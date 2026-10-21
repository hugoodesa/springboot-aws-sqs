package br.com.stapassoli.spring_sqs_aws.service;

import br.com.stapassoli.spring_sqs_aws.domain.dto.EventMessageDTO;
import br.com.stapassoli.spring_sqs_aws.domain.entity.InvoiceEntity;
import br.com.stapassoli.spring_sqs_aws.domain.enums.InvoiceStatus;
import br.com.stapassoli.spring_sqs_aws.domain.repository.InvoiceRepository;
import br.com.stapassoli.spring_sqs_aws.exception.NonRetryableException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class InvoiceServiceTest {

    @Mock
    private InvoiceRepository invoiceRepository;

    @InjectMocks
    private InvoiceService invoiceService;

    @Test
    @DisplayName("1. EventBridge com invoiceId: localiza a nota e atualiza status para Pago")
    void shouldUpdateInvoiceToPaidWhenInvoiceIdIsPresent() {
        InvoiceEntity invoice = pendingInvoice("CHAVEPORTALNACIONAL");
        when(invoiceRepository.findByInvoiceId("CHAVEPORTALNACIONAL")).thenReturn(Optional.of(invoice));
        when(invoiceRepository.save(any(InvoiceEntity.class))).thenAnswer(invocation -> invocation.getArgument(0));

        Map<String, Object> detail = new HashMap<>();
        detail.put("invoiceId", "CHAVEPORTALNACIONAL");
        detail.put("payment", List.of(Map.of("key", "numeroSeqPG")));
        detail.put("invoiceStatus", "Pendente");

        Optional<InvoiceEntity> result = invoiceService.updatePaymentStatusToPaid(eventBridge(detail));

        assertThat(result).isPresent();
        assertThat(result.get().getStatus()).isEqualTo(InvoiceStatus.PAGO.getDescription());
        assertThat(result.get().getLastPaymentKey()).isEqualTo("numeroSeqPG");
        assertThat(result.get().getInvoiceId()).isEqualTo("CHAVEPORTALNACIONAL");

        ArgumentCaptor<InvoiceEntity> captor = ArgumentCaptor.forClass(InvoiceEntity.class);
        verify(invoiceRepository).save(captor.capture());
        assertThat(captor.getValue().getStatus()).isEqualTo("Pago");
        verify(invoiceRepository, never()).findByDocumentAndIssueDateAndTypeAndNumberAndSerie(
                anyString(), anyString(), anyString(), anyString(), anyString());
    }

    @Test
    @DisplayName("2. Sem invoiceId e com chave composta completa: localiza e atualiza para Pago")
    void shouldUpdateInvoiceToPaidUsingCompleteCompositeKey() {
        InvoiceEntity invoice = pendingInvoice(null);
        when(invoiceRepository.findByDocumentAndIssueDateAndTypeAndNumberAndSerie(
                "12345678000199", "2026-05-08", "NFS-e", "1001", "1"
        )).thenReturn(Optional.of(invoice));
        when(invoiceRepository.save(any(InvoiceEntity.class))).thenAnswer(invocation -> invocation.getArgument(0));

        Map<String, Object> detail = compositeKeyDetail();
        detail.put("payment", List.of(Map.of("key", "seq-88")));

        Optional<InvoiceEntity> result = invoiceService.updatePaymentStatusToPaid(eventBridge(detail));

        assertThat(result).isPresent();
        assertThat(result.get().getStatus()).isEqualTo(InvoiceStatus.PAGO.getDescription());
        assertThat(result.get().getLastPaymentKey()).isEqualTo("seq-88");
        verify(invoiceRepository, never()).findByInvoiceId(anyString());
    }

    @Test
    @DisplayName("3. Com invoiceId e sem campos da chave composta: processa normalmente")
    void shouldProcessUsingInvoiceIdWithoutCompositeFields() {
        InvoiceEntity invoice = pendingInvoice("INV-ONLY");
        when(invoiceRepository.findByInvoiceId("INV-ONLY")).thenReturn(Optional.of(invoice));
        when(invoiceRepository.save(any(InvoiceEntity.class))).thenAnswer(invocation -> invocation.getArgument(0));

        Map<String, Object> detail = new HashMap<>();
        detail.put("invoiceId", "INV-ONLY");
        detail.put("payment", List.of(Map.of("key", "pg-1")));

        Optional<InvoiceEntity> result = invoiceService.updatePaymentStatusToPaid(eventBridge(detail));

        assertThat(result).isPresent();
        assertThat(result.get().getStatus()).isEqualTo(InvoiceStatus.PAGO.getDescription());
        verify(invoiceRepository).findByInvoiceId("INV-ONLY");
        verify(invoiceRepository, never()).findByDocumentAndIssueDateAndTypeAndNumberAndSerie(
                anyString(), anyString(), anyString(), anyString(), anyString());
    }

    @Test
    @DisplayName("4. Sem invoiceId e com chave composta incompleta: rejeita o processamento")
    void shouldRejectWhenCompositeKeyIsIncomplete() {
        Map<String, Object> detail = new HashMap<>();
        detail.put("document", "12345678000199");
        detail.put("issueDate", "2026-05-08");
        detail.put("type", "NFS-e");
        detail.put("number", "1001");
        detail.put("payment", List.of(Map.of("key", "pg-1")));

        assertThatThrownBy(() -> invoiceService.updatePaymentStatusToPaid(eventBridge(detail)))
                .isInstanceOf(NonRetryableException.class)
                .hasMessageContaining("chave composta completa");

        verify(invoiceRepository, never()).save(any());
        verify(invoiceRepository, never()).findByInvoiceId(anyString());
        verify(invoiceRepository, never()).findByDocumentAndIssueDateAndTypeAndNumberAndSerie(
                anyString(), anyString(), anyString(), anyString(), anyString());
    }

    @Test
    @DisplayName("5. Mensagem sem detail: não realiza atualizações")
    void shouldNotUpdateWhenDetailIsMissing() {
        EventMessageDTO eventWithoutDetail = new EventMessageDTO(
                "IdHubdeEventos",
                "finance.invoice.bank-return",
                "event-hub",
                Instant.parse("2026-05-08T12:57:08Z"),
                null,
                null
        );

        Optional<InvoiceEntity> result = invoiceService.updatePaymentStatusToPaid(eventWithoutDetail);

        assertThat(result).isEmpty();
        verify(invoiceRepository, never()).save(any());
        verify(invoiceRepository, never()).findByInvoiceId(anyString());
    }

    @Test
    @DisplayName("6. payment ausente ou vazio: rejeita o processamento")
    void shouldRejectWhenPaymentIsMissingOrEmpty() {
        Map<String, Object> withoutPayment = new HashMap<>();
        withoutPayment.put("invoiceId", "INV-1");

        assertThatThrownBy(() -> invoiceService.updatePaymentStatusToPaid(eventBridge(withoutPayment)))
                .isInstanceOf(NonRetryableException.class)
                .hasMessageContaining("payment");

        Map<String, Object> emptyPayment = new HashMap<>();
        emptyPayment.put("invoiceId", "INV-1");
        emptyPayment.put("payment", List.of());

        assertThatThrownBy(() -> invoiceService.updatePaymentStatusToPaid(eventBridge(emptyPayment)))
                .isInstanceOf(NonRetryableException.class)
                .hasMessageContaining("payment");

        verify(invoiceRepository, never()).save(any());
        verify(invoiceRepository, never()).findByInvoiceId(anyString());
    }

    @Test
    @DisplayName("7. Item de pagamento sem key: rejeita o processamento")
    void shouldRejectWhenPaymentItemHasNoKey() {
        Map<String, Object> detail = new HashMap<>();
        detail.put("invoiceId", "INV-1");
        detail.put("payment", List.of(Map.of("amount", "10.00")));

        assertThatThrownBy(() -> invoiceService.updatePaymentStatusToPaid(eventBridge(detail)))
                .isInstanceOf(NonRetryableException.class)
                .hasMessageContaining("key");

        verify(invoiceRepository, never()).save(any());
    }

    @Test
    @DisplayName("8. Nota fiscal não encontrada: lança NonRetryableException")
    void shouldThrowWhenInvoiceIsNotFound() {
        when(invoiceRepository.findByInvoiceId("INEXISTENTE")).thenReturn(Optional.empty());

        Map<String, Object> detail = new HashMap<>();
        detail.put("invoiceId", "INEXISTENTE");
        detail.put("payment", List.of(Map.of("key", "pg-1")));

        assertThatThrownBy(() -> invoiceService.updatePaymentStatusToPaid(eventBridge(detail)))
                .isInstanceOf(NonRetryableException.class)
                .hasMessageContaining("não encontrada");

        verify(invoiceRepository, never()).save(any());
    }

    @Test
    @DisplayName("9. Atributos extras em detail não impedem o processamento")
    void shouldIgnoreUnusedDetailAttributes() {
        InvoiceEntity invoice = pendingInvoice("CHAVE-EXTRA");
        when(invoiceRepository.findByInvoiceId("CHAVE-EXTRA")).thenReturn(Optional.of(invoice));
        when(invoiceRepository.save(any(InvoiceEntity.class))).thenAnswer(invocation -> invocation.getArgument(0));

        Map<String, Object> detail = new HashMap<>();
        detail.put("invoiceId", "CHAVE-EXTRA");
        detail.put("payment", List.of(Map.of("key", "numeroSeqPG", "bank", "001")));
        detail.put("invoiceStatus", "Cancelado");
        detail.put("correlationId", "corr-xyz");
        detail.put("ignoredField", "qualquer-valor");

        Optional<InvoiceEntity> result = invoiceService.updatePaymentStatusToPaid(eventBridge(detail));

        assertThat(result).isPresent();
        assertThat(result.get().getStatus()).isEqualTo(InvoiceStatus.PAGO.getDescription());
        assertThat(result.get().getStatus()).isNotEqualTo("Cancelado");
    }

    @Test
    @DisplayName("10. Reprocessamento de mensagem válida não altera novamente a nota já paga")
    void shouldBeSafeOnReprocessingAlreadyPaidInvoice() {
        InvoiceEntity invoice = pendingInvoice("INV-REPLAY");
        when(invoiceRepository.findByInvoiceId("INV-REPLAY")).thenReturn(Optional.of(invoice));
        when(invoiceRepository.save(any(InvoiceEntity.class))).thenAnswer(invocation -> invocation.getArgument(0));

        Map<String, Object> detail = new HashMap<>();
        detail.put("invoiceId", "INV-REPLAY");
        detail.put("payment", List.of(Map.of("key", "pg-replay")));

        EventMessageDTO event = eventBridge(detail);

        Optional<InvoiceEntity> first = invoiceService.updatePaymentStatusToPaid(event);
        Optional<InvoiceEntity> second = invoiceService.updatePaymentStatusToPaid(event);

        assertThat(first).isPresent();
        assertThat(second).isPresent();
        assertThat(first.get().getStatus()).isEqualTo(InvoiceStatus.PAGO.getDescription());
        assertThat(second.get().getStatus()).isEqualTo(InvoiceStatus.PAGO.getDescription());
        verify(invoiceRepository, times(1)).save(any(InvoiceEntity.class));
    }

    private EventMessageDTO eventBridge(Map<String, Object> detail) {
        return new EventMessageDTO(
                "IdHubdeEventos",
                "finance.invoice.bank-return",
                "event-hub",
                Instant.parse("2026-05-08T12:57:08Z"),
                null,
                detail
        );
    }

    private Map<String, Object> compositeKeyDetail() {
        Map<String, Object> detail = new HashMap<>();
        detail.put("document", "12345678000199");
        detail.put("issueDate", "2026-05-08");
        detail.put("type", "NFS-e");
        detail.put("number", "1001");
        detail.put("serie", "1");
        return detail;
    }

    private InvoiceEntity pendingInvoice(String invoiceId) {
        return InvoiceEntity.builder()
                .id(UUID.randomUUID())
                .invoiceId(invoiceId)
                .document("12345678000199")
                .issueDate("2026-05-08")
                .type("NFS-e")
                .number("1001")
                .serie("1")
                .status(InvoiceStatus.PENDENTE.getDescription())
                .createdAt(Instant.now())
                .build();
    }
}
