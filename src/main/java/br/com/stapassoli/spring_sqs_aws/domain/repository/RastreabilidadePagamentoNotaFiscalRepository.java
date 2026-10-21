package br.com.stapassoli.spring_sqs_aws.domain.repository;

import br.com.stapassoli.spring_sqs_aws.domain.entity.RastreabilidadePagamentoNotaFiscalEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.util.Optional;

@Repository
public interface RastreabilidadePagamentoNotaFiscalRepository extends JpaRepository<RastreabilidadePagamentoNotaFiscalEntity, Long> {

    Optional<RastreabilidadePagamentoNotaFiscalEntity> findByCodigoPortalNacional(String codigoPortalNacional);

    Optional<RastreabilidadePagamentoNotaFiscalEntity> findByTipoNotaFiscalAndNumeroNotaFiscalAndNumeroSerieNotaFiscalAndDataEmissaoNotaFiscal(
            String tipoNotaFiscal,
            String numeroNotaFiscal,
            String numeroSerieNotaFiscal,
            LocalDate dataEmissaoNotaFiscal
    );
}
