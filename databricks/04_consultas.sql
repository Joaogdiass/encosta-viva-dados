-- Encosta Viva: as 5 consultas do requisito 07.
-- Limiar de 80 mm é ilustrativo. Calibre com o PPDC antes de citar como referência.

-- Consulta 1: setores com chuva de 72 h acima do limiar de atenção nas últimas 24 h de dados
WITH ultima AS (SELECT MAX(data_hora) AS fim FROM workspace.gs.dim_tempo)
SELECT s.setor_id, s.nome, s.municipio_nome,
       MAX(f.chuva_72h) AS maior_chuva_72h_mm,
       COUNT(*) AS horas_acima_do_limiar
FROM workspace.gs.fato_risco_setor f
JOIN workspace.gs.dim_tempo t ON f.tempo_id = t.tempo_id
JOIN workspace.gs.dim_setor s ON f.setor_id = s.setor_id
CROSS JOIN ultima u
WHERE t.data_hora > u.fim - INTERVAL 24 HOURS
  AND f.chuva_72h >= 80
GROUP BY s.setor_id, s.nome, s.municipio_nome
ORDER BY maior_chuva_72h_mm DESC;

-- Consulta 2: ranking de setores por horas em nível Alerta ou superior
SELECT s.setor_id, s.nome, s.grau_risco,
       SUM(CASE WHEN f.nivel_ordem >= 3 THEN 1 ELSE 0 END) AS horas_em_alerta,
       SUM(CASE WHEN f.nivel_ordem = 4 THEN 1 ELSE 0 END) AS horas_em_alerta_maximo,
       COUNT(*) AS horas_monitoradas
FROM workspace.gs.fato_risco_setor f
JOIN workspace.gs.dim_setor s ON f.setor_id = s.setor_id
GROUP BY s.setor_id, s.nome, s.grau_risco
ORDER BY horas_em_alerta DESC, horas_em_alerta_maximo DESC;

-- Consulta 3: correlação entre chuva e umidade do solo, agora e com atraso de 3 e 6 horas
WITH serie AS (
  SELECT f.setor_id, f.umidade_solo_media AS umidade,
         f.chuva_1h AS chuva_agora,
         LAG(f.chuva_1h, 3) OVER (PARTITION BY f.setor_id ORDER BY f.tempo_id) AS chuva_3h_antes,
         LAG(f.chuva_1h, 6) OVER (PARTITION BY f.setor_id ORDER BY f.tempo_id) AS chuva_6h_antes
  FROM workspace.gs.fato_risco_setor f
  WHERE f.umidade_solo_media IS NOT NULL
)
SELECT setor_id,
       ROUND(CORR(chuva_agora, umidade), 3) AS corr_mesma_hora,
       ROUND(CORR(chuva_3h_antes, umidade), 3) AS corr_3h_antes,
       ROUND(CORR(chuva_6h_antes, umidade), 3) AS corr_6h_antes
FROM serie
GROUP BY setor_id
ORDER BY setor_id;

-- Consulta 4: sensores sem leitura há mais de 30 minutos, em relação à leitura mais recente da rede
SELECT sn.sensor_id, s.nome, sn.ultima_leitura, sn.minutos_sem_leitura, sn.bateria_pct, sn.status
FROM workspace.gs.dim_sensor sn
JOIN workspace.gs.dim_setor s ON sn.setor_id = s.setor_id
WHERE sn.minutos_sem_leitura > 30
ORDER BY sn.minutos_sem_leitura DESC;

-- Consulta 5 (IMPACTO): população em setores alertados antes do pico de chuva e antecedência média.
-- Atenção: use só para demonstrar o método enquanto os eventos forem simulados.
WITH dia AS (
  SELECT f.setor_id, t.data, t.data_hora, f.chuva_1h, f.nivel_ordem, f.fonte_chuva
  FROM workspace.gs.fato_risco_setor f
  JOIN workspace.gs.dim_tempo t ON f.tempo_id = t.tempo_id
),
pico AS (
  SELECT setor_id, data,
         MAX_BY(data_hora, chuva_1h) AS hora_pico,
         MAX(chuva_1h) AS chuva_pico,
         MAX_BY(fonte_chuva, chuva_1h) AS fonte_pico
  FROM dia
  GROUP BY setor_id, data
  HAVING MAX(chuva_1h) >= 15
),
primeiro_alerta AS (
  SELECT d.setor_id, d.data, MIN(d.data_hora) AS hora_alerta
  FROM dia d
  WHERE d.nivel_ordem >= 3
  GROUP BY d.setor_id, d.data
)
SELECT p.setor_id, s.nome, p.data, p.chuva_pico, p.fonte_pico, a.hora_alerta, p.hora_pico,
       s.populacao_estimada,
       (UNIX_TIMESTAMP(p.hora_pico) - UNIX_TIMESTAMP(a.hora_alerta)) / 60 AS antecedencia_min
FROM pico p
JOIN primeiro_alerta a ON p.setor_id = a.setor_id AND p.data = a.data
JOIN workspace.gs.dim_setor s ON p.setor_id = s.setor_id
WHERE a.hora_alerta < p.hora_pico
ORDER BY p.data, p.setor_id;
