package br.com.stapassoli.spring_sqs_aws.domain.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "rastreabilidade_pagamento_nota_fiscal")
public class RastreabilidadePagamentoNotaFiscalEntity {

    @Id
    @Column(name = "codigo_sequencial_nota_fiscal")
    private Long codigoSequencialNotaFiscal;

    @Column(name = "codigo_portal_nacional")
    private String codigoPortalNacional;

    @Column(name = "codigo_nota_fiscal_referencia")
    private String codigoNotaFiscalReferencia;

    @Column(name = "data_emissao_nota_fiscal")
    private LocalDate dataEmissaoNotaFiscal;

    @Column(name = "data_autorizacao")
    private LocalDateTime dataAutorizacao;

    @Column(name = "codigo_cnpj_emitente")
    private String codigoCnpjEmitente;

    @Column(name = "codigo_cnpj_destinatario")
    private String codigoCnpjDestinatario;

    @Column(name = "codigo_municipio_origem")
    private String codigoMunicipioOrigem;

    @Column(name = "codigo_uf_origem")
    private String codigoUfOrigem;

    @Column(name = "codigo_municipio_destino")
    private String codigoMunicipioDestino;

    @Column(name = "codigo_uf_destino")
    private String codigoUfDestino;

    @Column(name = "valor_total_produtos")
    private BigDecimal valorTotalProdutos;

    @Column(name = "valor_total_servicos")
    private BigDecimal valorTotalServicos;

    @Column(name = "valor_total_frete")
    private BigDecimal valorTotalFrete;

    @Column(name = "valor_total_seguro")
    private BigDecimal valorTotalSeguro;

    @Column(name = "valor_total_desconto")
    private BigDecimal valorTotalDesconto;

    @Column(name = "valor_total_documento")
    private BigDecimal valorTotalDocumento;

    @Column(name = "valor_total_icms")
    private BigDecimal valorTotalIcms;

    @Column(name = "valor_total_ipi")
    private BigDecimal valorTotalIpi;

    @Column(name = "valor_total_pis")
    private BigDecimal valorTotalPis;

    @Column(name = "valor_total_issqn")
    private BigDecimal valorTotalIssqn;

    @Column(name = "valor_base_calculo_ibs")
    private BigDecimal valorBaseCalculoIbs;

    @Column(name = "valor_base_calculo_cbs")
    private BigDecimal valorBaseCalculoCbs;

    @Column(name = "aliquota_ibs")
    private BigDecimal aliquotaIbs;

    @Column(name = "aliquota_cbs")
    private BigDecimal aliquotaCbs;

    @Column(name = "valor_ibs")
    private BigDecimal valorIbs;

    @Column(name = "valor_cbs")
    private BigDecimal valorCbs;

    @Column(name = "valor_credito_ibs")
    private BigDecimal valorCreditoIbs;

    @Column(name = "valor_credito_cbs")
    private BigDecimal valorCreditoCbs;

    @Column(name = "descricao_servico")
    private String descricaoServico;

    @Column(name = "tipo_nota_fiscal")
    private String tipoNotaFiscal;

    @Column(name = "numero_nota_fiscal")
    private String numeroNotaFiscal;

    @Column(name = "numero_serie_nota_fiscal")
    private String numeroSerieNotaFiscal;

    @Column(name = "status_nota_fiscal")
    private String statusNotaFiscal;

    @Column(name = "json_nota_fiscal", columnDefinition = "TEXT")
    private String jsonNotaFiscal;

    @Column(name = "tipo_negocio")
    private String tipoNegocio;

    @Column(name = "data_manutencao")
    private LocalDateTime dataManutencao;

    @Column(name = "codigo_usuario_manutencao")
    private String codigoUsuarioManutencao;
}
