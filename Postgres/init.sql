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


SELECT pg_create_logical_replication_slot('python_slot', 'test_decoding');

CREATE PUBLICATION my_python_pub FOR TABLE financeiro.movimentos_financeiros, financeiro.clientes;
