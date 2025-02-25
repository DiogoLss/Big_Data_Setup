CREATE SCHEMA IF NOT EXISTS financeiro;

CREATE TABLE IF NOT EXISTS financeiro.movimentos_financeiros (
    id SERIAL PRIMARY KEY,
    data_movimento DATE,
    valor NUMERIC(10,2),
    cliente_id INT
);

CREATE TABLE IF NOT EXISTS financeiro.clientes (
    cliente_id SERIAL PRIMARY KEY,
    nome VARCHAR(100),
    cidade VARCHAR(50)
);