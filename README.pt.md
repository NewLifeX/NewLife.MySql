# NewLife.MySql — Driver MySQL de Alto Desempenho em C# Puro

[![NuGet](https://img.shields.io/nuget/v/NewLife.MySql.svg)](https://www.nuget.org/packages/NewLife.MySql)
[![License](https://img.shields.io/github/license/NewLifeX/NewLife.MySql)](https://github.com/NewLifeX/NewLife.MySql/blob/master/LICENSE)
[![Downloads](https://img.shields.io/nuget/dt/NewLife.MySql.svg)](https://www.nuget.org/packages/NewLife.MySql)

> 🌐 [中文](Readme.MD) | [English](README.en.md) | [日本語](README.ja.md) | [한국어](README.ko.md) | [Español](README.es.md) | **Português**

**NewLife.MySql** é um driver ADO.NET para MySQL em C# puro desenvolvido pela equipe NewLife. Implementa o protocolo de rede MySQL (Protocol Version 10) diretamente sobre TCP, com **zero dependências de terceiros**, **suporte completo a async/await** e **licença MIT** para uso comercial sem preocupações.

Sua inovadora **execução em lote pipeline** oferece desempenho DML em lote **2–3× mais rápido** comparado à execução linha por linha.

---

## Comparação de recursos

| Funcionalidade | NewLife.MySql | MySqlConnector | MySql.Data (Oracle) |
|---------|:---:|:---:|:---:|
| Licença | **MIT** ✅ | MIT ✅ | GPLv2 ⚠️ |
| Dependências | **0** (apenas NewLife.Core) | 1 | 6 |
| IO Assíncrono real | ✅ | ✅ | ❌ |
| Lote pipeline | ✅ | ❌ | ❌ |
| Lote por array | ✅ | ❌ | ❌ |
| Lote por dicionário | ✅ | ❌ | ❌ |
| MySqlBulkCopy | ✅ | ✅ | ✅ |
| Compressão (zlib/zstd) | ✅ | ✅ | ❌ |
| Unix Socket | ✅ | ✅ | ❌ |
| Autenticação WebAuthn | ✅ | ✅ | ❌ |
| DbDataSource | ✅ | ❌ | ❌ |
| OceanBase / TiDB / Aurora / Doris | ✅ Detecção automática | ❌ | ❌ |
| Frameworks suportados | net45 ~ net10 | net462+ | net462+ |

---

## Desempenho

> Ambiente de teste: .NET 10 + MySQL 8.0.39 localhost (Windows), TCP 127.0.0.1:3306

### DML em lote: 10.000 linhas (ms, menor é melhor)

| Operação | NewLife Pipeline(tx) | MySql.Data Batch(tx) | MySqlConnector Batch(tx) |
|------|------:|------:|------:|
| INSERT | **899** | 1.927 | 1.906 |
| UPDATE | **710** | 2.265 | 2.041 |
| DELETE | **661** | 1.961 | 1.767 |

### Aceleração com pipeline (1.000 linhas)

| Cenário | Linha a linha | Pipeline + Tx | Aceleração |
|----------|----------:|-------------:|-------:|
| INSERT | 437ms | 54ms | **8.1×** |

---

## Início rápido

### Instalação

```shell
dotnet add package NewLife.MySql
```

### Conexão

```csharp
using var conn = new MySqlConnection("Server=localhost;Database=mydb;User Id=root;Password=pass;");
conn.Open();
```

### CRUD básico

```csharp
// Consulta
using var cmd = new MySqlCommand(conn, "SELECT id, name, age FROM users WHERE age > 18");
using var reader = cmd.ExecuteReader();
while (reader.Read())
{
    Console.WriteLine($"{reader.GetInt64(0)}: {reader.GetString(1)}, {reader.GetInt64(2)}");
}

// Escalar
var count = (Int64)cmd.ExecuteScalar("SELECT COUNT(*) FROM users");

// Não consulta
var rows = cmd.ExecuteNonQuery("INSERT INTO users(name, age) VALUES('Tom', 25)");
```

### Consultas parametrizadas

```csharp
using var cmd = new MySqlCommand(conn, "SELECT * FROM users WHERE name = @name AND age > @age");
cmd.Parameters.AddWithValue("name", "Tom");
cmd.Parameters.AddWithValue("age", 18);
using var reader = cmd.ExecuteReader();
```

### Transações

```csharp
using var tr = conn.BeginTransaction();
try
{
    conn.ExecuteNonQuery("INSERT INTO orders(product, qty) VALUES('Widget', 10)");
    conn.ExecuteNonQuery("UPDATE inventory SET qty = qty - 10 WHERE product = 'Widget'");
    tr.Commit();
}
catch
{
    tr.Rollback();
    throw;
}
```

### Operações em lote (Pipeline)

```csharp
var connStr = "Server=localhost;Database=mydb;...;Pipeline=true;";
using var conn = new MySqlConnection(connStr);
conn.Open();

using var cmd = new MySqlCommand(conn, "UPDATE users SET age=@age WHERE name=@name");
cmd.Parameters.AddWithValue("age", agesArray);    // Int32[10000]
cmd.Parameters.AddWithValue("name", namesArray);   // String[10000]
var totalAffected = cmd.ExecuteArrayBatch(10000);  // 2~3× mais rápido que a concorrência!
```

### API assíncrona

```csharp
await conn.OpenAsync();
using var reader = await cmd.ExecuteReaderAsync();
while (await reader.ReadAsync())
{
    Console.WriteLine(reader.GetString(0));
}
```

---

## Parâmetros da string de conexão

| Parâmetro | Alias | Padrão | Descrição |
|-----------|---------|---------|-------------|
| Server | DataSource, Data Source | — | Endereço do servidor |
| Port | — | 3306 | Número da porta |
| Database | — | — | Nome do banco de dados |
| UserID | Uid, User Id, User | — | Nome de usuário |
| Password | Pass, Pwd | — | Senha |
| ConnectionTimeout | Connection Timeout | 15 | Tempo limite em segundos |
| SslMode | Ssl Mode | None | None / Preferred / Required |
| UseServerPrepare | Use Server Prepare | false | Preparação no servidor |
| Pipeline | Pipelining | false | Execução em lote pipeline |
| MinPoolSize | Min Pool Size | 0 | Tamanho mínimo do pool |
| MaxPoolSize | Max Pool Size | 100 | Tamanho máximo do pool |

---

## Compatibilidade de frameworks

| TFM | Status |
|-----|--------|
| `net45` | ✅ O único driver MySQL moderno compatível com .NET 4.5 |
| `net461` | ✅ |
| `netstandard2.0` | ✅ |
| `netstandard2.1` | ✅ |
| `net6.0` | ✅ + API DbBatch |
| `net8.0` | ✅ |
| `net10.0` | ✅ Último .NET |

---

## Compatibilidade de bancos de dados

| Banco de dados | Detecção | Notas |
|----------|:---------:|-------|
| MySQL 5.x ~ 9.0+ | Automática | 3 métodos de autenticação |
| OceanBase | Detecção automática | CRUD completo + transações |
| TiDB | Detecção automática | CRUD completo + transações |
| Aurora / CloudSQL | Detecção automática | Compatível com AWS Aurora / GCP Cloud SQL |
| MariaDB | Básica | Sem suporte a `ed25519` |

---

## Licença

MIT License — uso gratuito para projetos pessoais e comerciais.

Copyright © 2002-2026 [NewLife](https://newlifex.com)
