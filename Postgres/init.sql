--Cria schema
CREATE SCHEMA IF NOT EXISTS financeiro;
--Cria função para armazenar timestamp (update e insert)
CREATE OR REPLACE FUNCTION set_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.modificado_em = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
--Cria tabelas
CREATE TABLE IF NOT EXISTS financeiro.movimentos_financeiros (
    id SERIAL PRIMARY KEY,
    data_movimento DATE,
    valor NUMERIC(10,2),
    cliente_id INT,
    modificado_em TIMESTAMP DEFAULT now();
);

CREATE TABLE IF NOT EXISTS financeiro.clientes (
    cliente_id SERIAL PRIMARY KEY,
    nome VARCHAR(100),
    cidade VARCHAR(50),
    modificado_em TIMESTAMP DEFAULT now();
);
--Cria triggers para armazenar timestamp nas linhas
CREATE TRIGGER update_timestamp_clientes
BEFORE UPDATE ON financeiro.clientes
FOR EACH ROW
EXECUTE FUNCTION set_timestamp();

CREATE TRIGGER update_timestamp_movimentos
BEFORE UPDATE ON financeiro.movimentos_financeiros
FOR EACH ROW
EXECUTE FUNCTION set_timestamp();

--ativa cdc
SELECT pg_create_logical_replication_slot('python_slot', 'test_decoding');
CREATE PUBLICATION my_python_pub FOR TABLE financeiro.movimentos_financeiros, financeiro.clientes;
