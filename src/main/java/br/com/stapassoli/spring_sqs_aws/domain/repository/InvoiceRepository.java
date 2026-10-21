package br.com.stapassoli.spring_sqs_aws.domain.repository;

import br.com.stapassoli.spring_sqs_aws.domain.entity.InvoiceEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

/**
 * Repositório JPA para consulta e atualização de Notas Fiscais.
 */
@Repository
public interface InvoiceRepository extends JpaRepository<InvoiceEntity, UUID> {

    /**
     * Localiza nota fiscal diretamente por invoiceId (REGRA 02).
     */
    Optional<InvoiceEntity> findByInvoiceId(String invoiceId);

    /**
     * Localiza nota fiscal pela chave composta completa (REGRA 03).
     */
    Optional<InvoiceEntity> findByDocumentAndIssueDateAndTypeAndNumberAndSerie(
            String document,
            String issueDate,
            String type,
            String number,
            String serie
    );
}
