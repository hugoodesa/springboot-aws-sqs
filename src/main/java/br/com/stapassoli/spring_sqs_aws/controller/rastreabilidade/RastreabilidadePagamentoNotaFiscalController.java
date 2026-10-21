package br.com.stapassoli.spring_sqs_aws.controller.rastreabilidade;

import br.com.stapassoli.spring_sqs_aws.domain.dto.EventMessageDTO;
import br.com.stapassoli.spring_sqs_aws.domain.entity.RastreabilidadePagamentoNotaFiscalEntity;
import br.com.stapassoli.spring_sqs_aws.domain.repository.RastreabilidadePagamentoNotaFiscalRepository;
import br.com.stapassoli.spring_sqs_aws.service.RastreabilidadePagamentoNotaFiscalService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/api/rastreabilidade")
@RequiredArgsConstructor
@Tag(name = "Rastreabilidade de pagamento", description = "Endpoints para processar pagamento e consultar registros de rastreabilidade da nota fiscal.")
public class RastreabilidadePagamentoNotaFiscalController {

    private static final String EXAMPLE_EVENTBRIDGE_PAYMENT = """
            {
              "id": "IdHubdeEventos",
              "detail-type": "finance.invoice.bank-return",
              "source": "event-hub",
              "time": "2026-05-08T12:57:08Z",
              "datacontenttype": "application/json",
              "detail": {
                "invoiceId": "35260112345678000199550010000000011234567890",
                "payment": [
                  {
                    "key": "PREFIXO+IDSEQPG-001"
                  }
                ],
                "invoiceStatus": "Pago"
              }
            }
            """;

    private static final String EXAMPLE_EVENTBRIDGE_PAYMENT_COMPOSITE_KEY = """
            {
              "id": "IdHubdeEventosCompuesto",
              "detail-type": "finance.invoice.bank-return",
              "source": "event-hub",
              "time": "2026-05-08T12:57:08Z",
              "datacontenttype": "application/json",
              "detail": {
                "document": "12345678000199",
                "issueDate": "2026-01-15",
                "type": "1",
                "number": "1",
                "serie": "1",
                "payment": [
                  {
                    "key": "PREFIXO+IDSEQPG-002"
                  }
                ]
              }
            }
            """;

    private final RastreabilidadePagamentoNotaFiscalService rastreabilidadePagamentoNotaFiscalService;
    private final RastreabilidadePagamentoNotaFiscalRepository rastreabilidadePagamentoNotaFiscalRepository;

    @PostMapping("/pagamento")
    @Operation(summary = "Processar pagamento da nota fiscal", description = "Recebe o payload do evento e atualiza a entidade de rastreabilidade conforme a mesma regra da InvoiceService.")
    @RequestBody(
            required = true,
            description = "Payload do evento para atualização do pagamento da nota fiscal.",
            content = @Content(
                    mediaType = "application/json",
                    schema = @Schema(implementation = EventMessageDTO.class),
                    examples = {
                            @ExampleObject(name = "Pagamento por invoiceId", value = EXAMPLE_EVENTBRIDGE_PAYMENT),
                            @ExampleObject(name = "Pagamento por chave composta", value = EXAMPLE_EVENTBRIDGE_PAYMENT_COMPOSITE_KEY)
                    }
            )
    )
    @ApiResponse(responseCode = "200", description = "Registro atualizado com sucesso")
    public ResponseEntity<RastreabilidadePagamentoNotaFiscalEntity> processarPagamento(
            @org.springframework.web.bind.annotation.RequestBody EventMessageDTO eventDTO
    ) {
        RastreabilidadePagamentoNotaFiscalEntity entity = rastreabilidadePagamentoNotaFiscalService
                .updatePaymentStatusToPaid(eventDTO)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NO_CONTENT, "Nenhuma rastreabilidade foi atualizada."));

        return ResponseEntity.ok(entity);
    }

    @GetMapping("/notas-fiscais")
    @Operation(summary = "Listar rastreabilidade das notas fiscais", description = "Retorna todos os registros persistidos da entidade de rastreabilidade.")
    @ApiResponse(
            responseCode = "200",
            description = "Lista de registros",
            content = @Content(
                    mediaType = "application/json",
                    examples = {
                            @ExampleObject(
                                    name = "Lista de rastreabilidade",
                                    value = """
                                            [
                                              {
                                                "codigoSequencialNotaFiscal": 1,
                                                "codigoPortalNacional": "35260112345678000199550010000000011234567890",
                                                "statusNotaFiscal": "Pago",
                                                "tipoNotaFiscal": "1",
                                                "numeroNotaFiscal": "1",
                                                "numeroSerieNotaFiscal": "1",
                                                "dataEmissaoNotaFiscal": "2026-01-15"
                                              }
                                            ]
                                            """
                            )
                    }
            )
    )
    public ResponseEntity<Page<RastreabilidadePagamentoNotaFiscalEntity>> listarNotasFiscais(
            @Parameter(description = "Número da página (base 0). Omitir juntamente com size para trazer todos os registros.")
            @RequestParam(required = false) Integer page,
            @Parameter(description = "Quantidade de registros por página. Omitir para trazer todos os registros.")
            @RequestParam(required = false) Integer size,
            @Parameter(description = "Campo de ordenação da entidade RastreabilidadePagamentoNotaFiscalEntity.")
            @RequestParam(defaultValue = "dataAutorizacao") String sort,
            @Parameter(description = "Direção da ordenação.")
            @RequestParam(defaultValue = "DESC") Sort.Direction direction
    ) {
        return ResponseEntity.ok(rastreabilidadePagamentoNotaFiscalRepository.findAll(toPageable(page, size, sort, direction)));
    }

    @GetMapping("/notas-fiscais/{codigoSequencialNotaFiscal}")
    @Operation(summary = "Consultar rastreabilidade por código sequencial", description = "Retorna um registro específico da entidade de rastreabilidade.")
    @ApiResponse(
            responseCode = "200",
            description = "Registro encontrado",
            content = @Content(
                    mediaType = "application/json",
                    examples = @ExampleObject(
                            name = "Rastreabilidade por código",
                            value = """
                                    {
                                      "codigoSequencialNotaFiscal": 1,
                                      "codigoPortalNacional": "35260112345678000199550010000000011234567890",
                                      "statusNotaFiscal": "Pago",
                                      "tipoNotaFiscal": "1",
                                      "numeroNotaFiscal": "1",
                                      "numeroSerieNotaFiscal": "1",
                                      "dataEmissaoNotaFiscal": "2026-01-15"
                                    }
                                    """
                    )
            )
    )
    @ApiResponse(responseCode = "404", description = "Registro não encontrado")
    public ResponseEntity<RastreabilidadePagamentoNotaFiscalEntity> consultarPorCodigo(
            @PathVariable Long codigoSequencialNotaFiscal
    ) {
        return rastreabilidadePagamentoNotaFiscalRepository.findById(codigoSequencialNotaFiscal)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    private Pageable toPageable(Integer page, Integer size, String sort, Sort.Direction direction) {
        Sort sortSpec = Sort.by(direction, sort);
        if (page == null && size == null) {
            return Pageable.unpaged(sortSpec);
        }
        int pageNumber = page == null ? 0 : Math.max(page, 0);
        int pageSize = size == null ? 20 : Math.max(size, 1);
        return PageRequest.of(pageNumber, pageSize, sortSpec);
    }
}
