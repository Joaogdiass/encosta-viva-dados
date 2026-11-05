# Relatório: Encosta Viva, componente de Dados

> Modelo na ordem exigida pelo PDF da disciplina. Substitua cada `[PRINT]` e `[PREENCHER]`, e coloque legenda em todos os prints.

## 1. Integrantes

| Nome completo | RM |
|---|---|
| Ricardo Mattos | 95906 |
| João Gabriel Dias | 99092 |
| Julio Zampieri | 98772 |

## 2. O produto e as perguntas

**Produto em uma frase:** a Encosta Viva avisa o risco de deslizamento por setor, explicando os fatores que pesaram, para moradores de encostas e para a Defesa Civil.

**Perguntas que o MVP responde:**

1. Qual é o nível de risco do meu setor agora, e por quê?
2. Quais setores têm chuva acumulada acima do limiar?
3. Quais setores passaram mais tempo em alerta?
4. Quais sensores estão sem enviar leitura?
5. Quantas pessoas estavam em setores alertados antes do pico de chuva, e com quanta antecedência?

## 3. Fluxo de dados

[PRINT do diagrama do README renderizado no GitHub]

*Legenda: fluxo da fonte ao MVP. O envio ao Volume é manual porque o Databricks Free Edition não se conecta ao Airflow.*

## 4. Fontes

| Fonte | Origem | Formato | Frequência | Natureza |
|---|---|---|---|---|
| Chuva e umidade | API Open-Meteo Forecast | JSON | Diária | Real |
| Previsão | API Open-Meteo Forecast | JSON | Diária | Real |
| População municipal | API IBGE, tabela 4709 | JSON | Diária; o dado não muda | Real |
| Sensores | Simulador | JSON Lines | Leitura a cada 15 min, arquivo diário | Simulada |
| Relatos | Simulador | JSON Lines | Diária | Simulada |
| Setores | Arquivo de configuração | JSON Lines | Diária | Ilustrativa |
| Cadastro | Simulador | JSON Lines | Diária | Fictícia |

## 5. Requisitos

### Requisito 01: fontes
Duas fontes externas reais (Open-Meteo e IBGE), ligadas ao problema porque chuva e umidade antecedem deslizamentos, e a população dimensiona o impacto.
[PRINT de `amostras/` ou da resposta da API]

### Requisito 02: DAG agendado
O DAG `gs_coleta_encosta` roda todo dia às 6h e grava um arquivo por coleta, com a data no nome.
[PRINT da pasta `chegada/`]

### Requisito 03: paralelismo, verificação e chaves
Seis tarefas de coleta rodam em paralelo, e `conferir_chegada` falha se faltar arquivo. O token do Telegram fica em Variables.
[PRINT das Variables, sem mostrar o valor]

### Requisito 04: lotes
[PRINT do Volume no lote 1, com as datas]
[PRINT do Volume no lote 2, com as datas]

### Requisito 05: Bronze
O Auto Loader lê só os arquivos novos, e o checkpoint impede duplicação.
[PRINT do resultado de `01_bronze`]

### Requisito 06: Prata
Tipos e fuso corrigidos, nulos e inválidos tratados, e MERGE pela chave.

| Tabela | Regra | Linhas barradas |
|---|---|---|
| [PREENCHER a partir de `controle_qualidade`] | | |

[PRINT de `controle_qualidade`]

### Requisito 07: Ouro
Fato `fato_risco_setor` (setor e hora) e dimensões `dim_setor`, `dim_tempo`, `dim_fonte`, `dim_sensor` e `dim_morador_cadastro`.
[PRINT das tabelas no Catalog]

### Requisito 08: LGPD e MVP
Nome e telefone não passam da Bronze. A `dim_morador_cadastro` guarda só o hash. O MVP lê `fato_risco_setor`.
[PRINT da `dim_morador_cadastro`]
[PRINT da tela do MVP]

### Requisito 09: repositório
[PRINT do repositório público, com o README]

## 6. Airflow

[PRINT da visão Graph com execução concluída]
[PRINT da visão Grid com várias execuções concluídas]

## 7. Contagens da Bronze

| Tabela | Após lote 1 | Após lote 2 | Após 2ª execução |
|---|---|---|---|
| bronze_chuva | [PREENCHER] | [PREENCHER] | [PREENCHER] |
| bronze_previsao | [PREENCHER] | [PREENCHER] | [PREENCHER] |
| bronze_ibge | [PREENCHER] | [PREENCHER] | [PREENCHER] |
| bronze_sensor | [PREENCHER] | [PREENCHER] | [PREENCHER] |
| bronze_relatos | [PREENCHER] | [PREENCHER] | [PREENCHER] |
| bronze_setores | [PREENCHER] | [PREENCHER] | [PREENCHER] |
| bronze_cadastro | [PREENCHER] | [PREENCHER] | [PREENCHER] |

[PRINT de `controle_contagens`]

## 8. Consultas SQL

Para cada uma, cole a consulta de `databricks/04_consultas.sql`, o print do resultado e a interpretação com os números reais.

1. **Setores acima do limiar de chuva:** [PRINT] [PREENCHER interpretação]
2. **Ranking de horas em alerta:** [PRINT] [PREENCHER interpretação]
3. **Correlação entre chuva e umidade:** [PRINT] [PREENCHER interpretação]
4. **Sensores sem leitura:** [PRINT] [PREENCHER interpretação]
5. **Impacto (população e antecedência):** [PRINT] [PREENCHER interpretação, deixando claro que os eventos são simulados]

## 9. Conclusão técnica

[PREENCHER: o que funcionou, as limitações de `docs/dados_simulados.md` e os próximos passos]

**Repositório:** [PREENCHER link público]
