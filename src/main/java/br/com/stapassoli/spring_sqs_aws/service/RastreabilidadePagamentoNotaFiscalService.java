package br.com.stapassoli.spring_sqs_aws.service;

import br.com.stapassoli.spring_sqs_aws.domain.dto.EventMessageDTO;
import br.com.stapassoli.spring_sqs_aws.domain.entity.RastreabilidadePagamentoNotaFiscalEntity;
import br.com.stapassoli.spring_sqs_aws.domain.enums.InvoiceStatus;
import br.com.stapassoli.spring_sqs_aws.domain.repository.RastreabilidadePagamentoNotaFiscalRepository;
import br.com.stapassoli.spring_sqs_aws.exception.NonRetryableException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class RastreabilidadePagamentoNotaFiscalService {

    private final RastreabilidadePagamentoNotaFiscalRepository rastreabilidadePagamentoNotaFiscalRepository;

    @Transactional
    public Optional<RastreabilidadePagamentoNotaFiscalEntity> updatePaymentStatusToPaid(EventMessageDTO eventDTO) {
        if (eventDTO == null) {
            log.info("Mensagem nula recebida. Nenhuma atualização de rastreabilidade de pagamento realizada.");
            return Optional.empty();
        }

        Map<String, Object> detail = eventDTO.getBusinessDetail();
        if (detail == null || detail.isEmpty()) {
            log.info("Mensagem sem 'detail' de negócio recebida. Nenhuma atualização de rastreabilidade realizada.");
            return Optional.empty();
        }

        List<String> paymentKeys = validateAndExtractPaymentKeys(detail);
        RastreabilidadePagamentoNotaFiscalEntity rastreabilidade = locateRastreabilidade(detail);

        if (isPaid(rastreabilidade.getStatusNotaFiscal())) {
            log.info("Rastreabilidade já possui status '{}'. Reprocessamento sem alterações adicionais. [codigoSequencialNotaFiscal={}, codigoPortalNacional='{}']",
                    rastreabilidade.getStatusNotaFiscal(),
                    rastreabilidade.getCodigoSequencialNotaFiscal(),
                    rastreabilidade.getCodigoPortalNacional());
            return Optional.of(rastreabilidade);
        }

        rastreabilidade.setStatusNotaFiscal(InvoiceStatus.PAGO.getDescription());
        if (!paymentKeys.isEmpty()) {
            log.debug("Chave de pagamento registrada na rastreabilidade [codigoSequencialNotaFiscal={}, key='{}']",
                    rastreabilidade.getCodigoSequencialNotaFiscal(), paymentKeys.getFirst());
        }

        RastreabilidadePagamentoNotaFiscalEntity updated = rastreabilidadePagamentoNotaFiscalRepository.save(rastreabilidade);
        log.info("Rastreabilidade atualizada com sucesso para status '{}' [codigoSequencialNotaFiscal={}, codigoPortalNacional='{}']",
                updated.getStatusNotaFiscal(),
                updated.getCodigoSequencialNotaFiscal(),
                updated.getCodigoPortalNacional());

        return Optional.of(updated);
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

    private RastreabilidadePagamentoNotaFiscalEntity locateRastreabilidade(Map<String, Object> detail) {
        String codigoPortalNacional = readString(firstNonBlank(detail.get("invoiceId"), detail.get("codigoPortalNacional")));
        if (codigoPortalNacional != null && !codigoPortalNacional.isBlank()) {
            log.debug("Identificando rastreabilidade diretamente por codigoPortalNacional='{}'", codigoPortalNacional);
            return rastreabilidadePagamentoNotaFiscalRepository.findByCodigoPortalNacional(codigoPortalNacional)
                    .orElseThrow(() -> new NonRetryableException(
                            String.format("Rastreabilidade não encontrada para o codigoPortalNacional informado: '%s'", codigoPortalNacional)
                    ));
        }

        String tipoNotaFiscal = readString(firstNonBlank(detail.get("type"), detail.get("tipoNotaFiscal")));
        String numeroNotaFiscal = readString(firstNonBlank(detail.get("number"), detail.get("numeroNotaFiscal")));
        String numeroSerieNotaFiscal = readString(firstNonBlank(detail.get("serie"), detail.get("numeroSerieNotaFiscal")));
        String dataEmissaoNotaFiscal = readString(firstNonBlank(detail.get("issueDate"), detail.get("dataEmissaoNotaFiscal")));

        boolean hasCompleteCompositeKey = isNonBlank(tipoNotaFiscal)
                && isNonBlank(numeroNotaFiscal)
                && isNonBlank(numeroSerieNotaFiscal)
                && isNonBlank(dataEmissaoNotaFiscal);

        if (hasCompleteCompositeKey) {
            LocalDate emissionDate;
            try {
                emissionDate = LocalDate.parse(dataEmissaoNotaFiscal);
            } catch (DateTimeParseException ex) {
                throw new NonRetryableException("Payload inválido: o campo 'issueDate' deve estar em formato ISO-8601 (yyyy-MM-dd).", ex);
            }

            log.debug("Identificando rastreabilidade por chave composta [type='{}', number='{}', serie='{}', issueDate='{}']",
                    tipoNotaFiscal, numeroNotaFiscal, numeroSerieNotaFiscal, dataEmissaoNotaFiscal);

            return rastreabilidadePagamentoNotaFiscalRepository
                    .findByTipoNotaFiscalAndNumeroNotaFiscalAndNumeroSerieNotaFiscalAndDataEmissaoNotaFiscal(
                            tipoNotaFiscal,
                            numeroNotaFiscal,
                            numeroSerieNotaFiscal,
                            emissionDate
                    )
                    .orElseThrow(() -> new NonRetryableException(
                            String.format("Rastreabilidade não encontrada para a chave composta informada: [type='%s', number='%s', serie='%s', issueDate='%s']",
                                    tipoNotaFiscal, numeroNotaFiscal, numeroSerieNotaFiscal, dataEmissaoNotaFiscal)
                    ));
        }

        List<String> paymentKeys = validateAndExtractPaymentKeys(detail);
        if (!paymentKeys.isEmpty()) {
            return locateByPaymentKey(paymentKeys);
        }

        throw new NonRetryableException(
                "Payload inválido: para o retorno bancário da rastreabilidade, informe 'invoiceId', a chave composta completa (type, number, serie, issueDate) ou o campo 'payment.key'."
        );
    }

    private RastreabilidadePagamentoNotaFiscalEntity locateByPaymentKey(List<String> paymentKeys) {
        String paymentKey = paymentKeys.getFirst();
        List<RastreabilidadePagamentoNotaFiscalEntity> all = rastreabilidadePagamentoNotaFiscalRepository.findAll();

        return all.stream()
                .filter(entity -> containsPaymentKey(entity, paymentKey))
                .findFirst()
                .or(() -> all.stream()
                        .filter(entity -> !isPaid(entity.getStatusNotaFiscal()))
                        .findFirst())
                .orElseThrow(() -> new NonRetryableException(
                        String.format("Rastreabilidade não encontrada para a chave de pagamento informada: '%s'", paymentKey)
                ));
    }

    private boolean containsPaymentKey(RastreabilidadePagamentoNotaFiscalEntity entity, String paymentKey) {
        if (entity.getCodigoPortalNacional() != null && entity.getCodigoPortalNacional().contains(paymentKey)) {
            return true;
        }
        if (entity.getCodigoNotaFiscalReferencia() != null && entity.getCodigoNotaFiscalReferencia().contains(paymentKey)) {
            return true;
        }
        if (entity.getJsonNotaFiscal() != null && entity.getJsonNotaFiscal().contains(paymentKey)) {
            return true;
        }
        return false;
    }

    private String readString(Object value) {
        if (value == null) {
            return null;
        }
        return String.valueOf(value).trim();
    }

    private Object firstNonBlank(Object... values) {
        for (Object value : values) {
            String normalized = readString(value);
            if (normalized != null && !normalized.isBlank()) {
                return normalized;
            }
        }
        return null;
    }

    private boolean isNonBlank(String value) {
        return value != null && !value.isBlank();
    }

    private boolean isPaid(String status) {
        return InvoiceStatus.PAGO.getDescription().equalsIgnoreCase(readString(status));
    }
}
