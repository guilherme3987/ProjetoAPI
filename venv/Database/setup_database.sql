-- Conectar ao banco
\c projetoapi

-- Criar tabela com estrutura idêntica ao DataFrame
CREATE TABLE dados_consolidados (
    id SERIAL PRIMARY KEY,
    data_importacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Colunas do DataFrame
    idvenda INTEGER,
    datavenda DATE,
    produto VARCHAR(255),
    categoria VARCHAR(100),
    quantidade INTEGER,
    precounitario DECIMAL(10,2),
    precototal DECIMAL(10,2),
    tipopagamento VARCHAR(50),
    cliente VARCHAR(255),
    emailcliente VARCHAR(255),
    marca VARCHAR(100),
    
    -- Metadados do sistema
    sheet_name VARCHAR(100),
    file_name VARCHAR(255),
    
    -- Campos para controle de duplicatas
    hash_linha VARCHAR(64)
);

-- Criar índices para melhor performance
CREATE INDEX idx_dados_idvenda ON dados_consolidados(idvenda);
CREATE INDEX idx_dados_datavenda ON dados_consolidados(datavenda);
CREATE INDEX idx_dados_produto ON dados_consolidados(produto);
CREATE INDEX idx_dados_categoria ON dados_consolidados(categoria);
CREATE INDEX idx_dados_cliente ON dados_consolidados(cliente);
CREATE INDEX idx_dados_file_name ON dados_consolidados(file_name);

-- Conceder permissões
GRANT ALL PRIVILEGES ON TABLE dados_consolidados TO api_user;
GRANT USAGE, SELECT ON SEQUENCE dados_consolidados_id_seq TO api_user;