package br.com.stapassoli.spring_sqs_aws.service;

import br.com.stapassoli.spring_sqs_aws.domain.dto.EventMessageDTO;
import br.com.stapassoli.spring_sqs_aws.domain.entity.InvoiceEntity;
import br.com.stapassoli.spring_sqs_aws.domain.enums.InvoiceStatus;
import br.com.stapassoli.spring_sqs_aws.domain.repository.InvoiceRepository;
import br.com.stapassoli.spring_sqs_aws.exception.NonRetryableException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Serviço responsável por processar o retorno bancário da Nota Fiscal:
 * validação dos dados de negócio, identificação da nota fiscal e atualização de status para 'Pago'.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class InvoiceService {

    private final InvoiceRepository invoiceRepository;

    /**
     * Processa o retorno bancário para a Nota Fiscal contida no evento.
     *
     * @param eventDTO DTO do evento recebido
     * @return Optional contendo a InvoiceEntity atualizada ou Optional.empty() se o evento não contiver 'detail'
     */
    @Transactional
    public Optional<InvoiceEntity> updatePaymentStatusToPaid(EventMessageDTO eventDTO) {
        if (eventDTO == null) {
            log.info("Mensagem nula recebida. Nenhuma atualização de nota fiscal realizada.");
            return Optional.empty();
        }

        Map<String, Object> detail = eventDTO.getBusinessDetail();
        if (detail == null || detail.isEmpty()) {
            log.info("Mensagem sem 'detail' de negócio recebida. Nenhuma atualização de nota fiscal realizada.");
            return Optional.empty();
        }

        List<String> paymentKeys = validateAndExtractPaymentKeys(detail);
        InvoiceEntity invoice = locateInvoice(detail);

        if (InvoiceStatus.PAGO.getDescription().equals(invoice.getStatus())) {
            log.info("Nota fiscal já possui status '{}'. Reprocessamento sem alterações adicionais. [id={}, invoiceId='{}']",
                    InvoiceStatus.PAGO.getDescription(), invoice.getId(), invoice.getInvoiceId());
            return Optional.of(invoice);
        }

        invoice.setStatus(InvoiceStatus.PAGO.getDescription());
        if (!paymentKeys.isEmpty()) {
            invoice.setLastPaymentKey(paymentKeys.getFirst());
        }

        InvoiceEntity updatedInvoice = invoiceRepository.save(invoice);
        log.info("Nota fiscal atualizada com sucesso para status '{}' [id={}, invoiceId='{}', chaveComposta={}]",
                updatedInvoice.getStatus(), updatedInvoice.getId(), updatedInvoice.getInvoiceId(),
                buildCompositeKeyLog(updatedInvoice));

        return Optional.of(updatedInvoice);
    }

    private List<String> validateAndExtractPaymentKeys(Map<String, Object> detail) {
        Object paymentObj = detail.get("payment");
        if (!(paymentObj instanceof List<?> paymentList) || paymentList.isEmpty()) {
            throw new NonRetryableException("Payload inválido: o campo 'payment' deve existir e conter pelo menos um item.");
        }

        List<String> keys = new ArrayList<>();
        for (Object item : paymentList) {
            if (!(item instanceof Map<?, ?> itemMap)) {
                throw new NonRetryableException("Payload inválido: cada item de 'payment' deve ser um objeto com o campo 'key'.");
            }
            String key = readString(itemMap.get("key"));
            if (key == null || key.isBlank()) {
                throw new NonRetryableException("Payload inválido: cada item de 'payment' deve possuir um 'key' válido.");
            }
            keys.add(key);
        }

        return keys;
    }

    private InvoiceEntity locateInvoice(Map<String, Object> detail) {
        String invoiceId = readString(detail.get("invoiceId"));

        if (invoiceId != null && !invoiceId.isBlank()) {
            log.debug("Identificando nota fiscal diretamente por invoiceId='{}'", invoiceId);
            return invoiceRepository.findByInvoiceId(invoiceId)
                    .orElseThrow(() -> new NonRetryableException(
                            String.format("Nota fiscal não encontrada para o invoiceId informado: '%s'", invoiceId)
                    ));
        }

        String document = readString(detail.get("document"));
        String issueDate = readString(detail.get("issueDate"));
        String type = readString(detail.get("type"));
        String number = readString(detail.get("number"));
        String serie = readString(detail.get("serie"));

        boolean hasCompleteCompositeKey = isNonBlank(document)
                && isNonBlank(issueDate)
                && isNonBlank(type)
                && isNonBlank(number)
                && isNonBlank(serie);

        if (!hasCompleteCompositeKey) {
            throw new NonRetryableException(
                    "Payload inválido: para o retorno bancário da nota fiscal, informe 'invoiceId' ou a chave composta completa (document, issueDate, type, number, serie)."
            );
        }

        log.debug("Identificando nota fiscal por chave composta [document='{}', issueDate='{}', type='{}', number='{}', serie='{}']",
                document, issueDate, type, number, serie);

        return invoiceRepository.findByDocumentAndIssueDateAndTypeAndNumberAndSerie(
                        document, issueDate, type, number, serie
                )
                .orElseThrow(() -> new NonRetryableException(
                        String.format("Nota fiscal não encontrada para a chave composta informada: [document='%s', issueDate='%s', type='%s', number='%s', serie='%s']",
                                document, issueDate, type, number, serie)
                ));
    }

    private String readString(Object value) {
        if (value == null) {
            return null;
        }
        return String.valueOf(value).trim();
    }

    private boolean isNonBlank(String value) {
        return value != null && !value.isBlank();
    }

    private String buildCompositeKeyLog(InvoiceEntity invoice) {
        return String.format("[doc=%s, issueDate=%s, type=%s, num=%s, serie=%s]",
                invoice.getDocument(), invoice.getIssueDate(), invoice.getType(), invoice.getNumber(), invoice.getSerie());
    }
}
