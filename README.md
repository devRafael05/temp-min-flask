# Teste de Ambiente Flask na VM

Este guia mostra como criar uma aplicação Flask mínima para validar se o ambiente da VM está funcionando corretamente, incluindo a conexão com o banco de dados usando o `DatabaseManager`.

---

## 1. Pré-requisitos na VM

Certifique-se de que a VM possui:

- **Python 3.8+** instalado
- **ODBC Driver 17 for SQL Server** instalado
- Acesso de rede ao servidor SQL Server configurado
- As variáveis de ambiente configuradas (veja passo 3)

---

## 2. Estrutura do Projeto de Teste

```
MIN_APP_FLASK/
├── app.py
├── .env
├── requirements.txt
├── README.md
└── helpers/
    └── DatabaseManager.py
```

---

## 3. Configuração do Arquivo `.env`

O arquivo `.env` já está configurado na raiz do projeto com as credenciais:

```env
SECRET_DB_USERNAME=mis.rafael.silva2
SECRET_DB_PASSWORD=Neobpo@#$%2030
SECRET_DB_DOMAIN=NEOBPO
SECRET_DB_SERVER=100.67.155.103
SECRET_DB_DATABASE=SIS_APP_MIS
SECRET_DB_PORT=1333
```

---

## 4. Instalar Dependências

Execute o comando:

```bash
pip install -r requirements.txt
```

---

## 5. Executar a Aplicação

Execute o comando:

```bash
python app.py
```

A aplicação estará disponível em: `http://localhost:5000`

---

## 6. Testar os Endpoints

### Health Check
```bash
curl http://localhost:5000/
```

**Resposta esperada:**
```json
{
    "status": "ok",
    "message": "Flask está rodando na VM!"
}
```

### Teste de Conexão com Banco
```bash
curl http://localhost:5000/test-db
```

**Resposta esperada (sucesso):**
```json
{
    "status": "success",
    "message": "Conexão com banco de dados OK!",
    "server_time": "2026-01-07 14:20:00"
}
```

### Listar Tabelas
```bash
curl http://localhost:5000/test-query
```

**Resposta esperada (sucesso):**
```json
{
    "status": "success",
    "tables": [
        {
            "TABLE_SCHEMA": "dbo",
            "TABLE_NAME": "exemplo_tabela"
        },
        ...
    ]
}
```

---

## 7. Troubleshooting

### Erro: "ODBC Driver 17 for SQL Server not found"
Instale o driver ODBC:
- Download: https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server

### Erro: "pywin32 não encontrado"
```bash
pip install pywin32
python -c "import win32security; print('OK')"
```

### Erro: "Não foi possível autenticar o usuário"
- Verifique se o usuário/senha no `.env` estão corretos
- Verifique se a VM tem acesso de rede ao domínio

### Erro: "Conexão recusada ao SQL Server"
- Verifique se a porta 1333 está liberada no firewall
- Teste conectividade: `telnet 100.67.155.103 1333`

### Erro: "Login failed for user"
- Verifique se as credenciais no `.env` estão corretas
- Verifique se o usuário tem permissões no banco de dados

---

## 8. Próximos Passos

Após validar que o ambiente está funcionando:

1. ✅ Flask rodando na VM
2. ✅ Conexão com banco de dados OK
3. ✅ Queries sendo executadas corretamente

Você pode então implantar a aplicação completa `MIS_APPS` seguindo a mesma configuração de ambiente.

---

## 9. Estrutura dos Endpoints

### `GET /`
- **Descrição**: Health check básico
- **Retorno**: Status da aplicação Flask

### `GET /test-db`
- **Descrição**: Testa autenticação e conexão com SQL Server
- **Retorno**: Data/hora do servidor de banco de dados

### `GET /test-query`
- **Descrição**: Executa uma query de exemplo
- **Retorno**: Lista das 10 primeiras tabelas do banco

---

## 10. Notas Importantes

- A porta configurada é **1333** (não a padrão 1433)
- A autenticação usa **Windows Authentication** via `pywin32`
- O `DatabaseManager` faz impersonation do usuário antes de conectar
- Todas as rotas incluem tratamento de erros com traceback completo
