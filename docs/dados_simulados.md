# Dados simulados e ilustrativos

O briefing aceita dados simulados quando a geração é justificada e documentada. O código está em `dags/encosta/simulador.py` e é determinístico: a mesma data gera sempre o mesmo arquivo.

| Dado | Natureza | Por que não é real | Como é gerado |
|---|---|---|---|
| Chuva e umidade do Open-Meteo | **Real** | - | API pública, sem chave |
| População municipal do IBGE | **Real** | - | Censo 2022, tabela 4709 |
| Leituras do sensor | Simulada | A equipe não tem sensores instalados em encostas | Uma leitura a cada 15 min, com umidade ligada à chuva acumulada em 72 h |
| Eventos de chuva extrema | Simulada | Novembro pode não ter chuva forte nos setores durante a coleta | Pico de 35 mm/h às 18h nas datas de `EVENTOS_SIMULADOS` |
| Relatos de moradores | Simulada | O app e a Visão não fazem parte desta entrega | Um relato ocasional e três confirmados em cada evento |
| Cadastro de moradores | Fictícia | Dados pessoais reais não podem ir para o repositório | Nomes genéricos e telefones com DDD 00, que não existe |
| Coordenadas dos setores | Aproximada | Definidas sem o mapeamento oficial | `dags/encosta/setores.py` |
| `grau_risco` e `populacao_estimada` | Ilustrativa | O IBGE consultado só tem dados por município | `dags/encosta/setores.py` |

## Erros inseridos de propósito

Cada sensor recebe, por dia, uma umidade de 130%, uma chuva negativa, um horário no futuro e uma leitura duplicada. O arquivo de relatos recebe um setor inexistente (`S99`). Assim, as regras de qualidade da Prata têm o que barrar, e a contagem fica demonstrável.

O S05 fica sem leitura a partir das 20h, simulando falha de energia do painel solar. Isso alimenta a consulta 4.

## Limitações que devem constar no relatório

1. **Resolução do Open-Meteo:** a grade é maior que uma encosta. Em um teste perto da Vila Sahy, o ponto devolvido ficou a cerca de 13 km do pedido e com elevação 0. Por isso, o nível usa o maior valor entre o modelo e o sensor.
2. **Subestimação de eventos extremos:** para 18 e 19/02/2023, o histórico do Open-Meteo somou 179,8 mm nesse ponto, muito abaixo dos mais de 680 mm citados no material do tema, que ainda precisam ser confirmados na fonte primária.
3. **Limiares:** os valores do motor de risco em `databricks/funcoes.py` são ilustrativos e precisam ser calibrados com PPDC, CEMADEN e IPT.
4. **Antecedência:** a antecedência da consulta 5 vem de eventos simulados e demonstra o método. Ela não mede o desempenho real do alerta.
