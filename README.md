# software-architecture-practice-market-data

Serviço `market-data` da [plataforma de trading](https://github.com/erichenriquesm/software-architecture-practice) (projeto de prática de arquitetura de software).

Dados de Mercado: profundidade do livro com precisão, histórico de negociações e estatísticas de 24 h, por projeções (CQRS).

## O que faz

Responde as consultas públicas de mercado a partir de projeções dos eventos do `matching`: profundidade agregada por faixa de preço, negociações e estatísticas (spread, mínimo, máximo, volume) numa janela móvel de 24 h. Não decide nada e não publica eventos; as projeções podem ser reconstruídas do zero pelo histórico que o `matching` republica (o broker só transporta).

A especificação completa (modelo, casos de uso, dados, testes) está na spec do serviço: [`sdd/services/market-data.md`](https://github.com/erichenriquesm/software-architecture-practice/blob/main/sdd/services/market-data.md). As regras de arquitetura valem para todos os serviços: [`sdd/01-principios.md`](https://github.com/erichenriquesm/software-architecture-practice/blob/main/sdd/01-principios.md).

## API

| Rota | O que faz |
|---|---|
| `GET /markets/:marketId` | spread, min, max, volume das últimas 24 h |
| `GET /markets/:marketId/trades` | negociações do mercado |
| `GET /markets/:marketId/depth?precision=` | compras e vendas agregadas por faixa de preço |

**gRPC interno:** Não tem.

## Eventos (RabbitMQ)

- **Publica:** Nenhum.
- **Consome:** `matching.tradeExecuted`, `matching.bookSnapshot` (e a fila `marketData.replay`, só no rebuild das projeções: `make projections-rebuild` em `backend/`)

Os contratos ficam em `src/infra/contracts/`: `published/` (este serviço é o dono) e `consumed/` (cópias do que ele consome, conferidas pelo `make check` do repositório guarda-chuva).

## Como rodar

Este serviço roda junto com a plataforma, a partir do [repositório guarda-chuva](https://github.com/erichenriquesm/software-architecture-practice), que tem a infra compartilhada (RabbitMQ, Redis, edge) e clona todos os serviços:

```bash
git clone https://github.com/erichenriquesm/software-architecture-practice.git
cd software-architecture-practice/backend
make clone
make up
```

Containers: `market-data-api` · `market-data-worker` (projeções) · `market-data-db`. Portas no host (só para depurar): REST 3005 · Postgres 5437.

Dentro da pasta do serviço (`backend/market-data/`):

| Comando | O que faz |
|---|---|
| `make test` | typecheck, lint, regras de arquitetura, testes de unidade e de integração (tudo em container) |
| `make migrate` · `make migrate-status` · `make migrate-rollback` · `make migration name=...` | migrações no estilo Laravel |
| `make deps` | instala o `node_modules` na sua máquina, só para o editor |

## Estrutura

```
src/
  application/   casos de uso, DTOs e contratos (interfaces)
  infra/         HTTP, mensageria, banco, plataforma técnica, contratos de eventos
test/
  unit/  integration/  fakes/  support/
docker/          Dockerfile e composes (só para ambiente local)
```
