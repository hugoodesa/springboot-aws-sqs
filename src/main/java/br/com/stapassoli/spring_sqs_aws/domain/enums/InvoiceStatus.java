package br.com.stapassoli.spring_sqs_aws.domain.enums;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Representa os estados possíveis do status de pagamento de uma Nota Fiscal.
 */
@Getter
@RequiredArgsConstructor
public enum InvoiceStatus {

    PENDENTE("Pendente"),
    PAGO("Pago");

    private final String description;
}
