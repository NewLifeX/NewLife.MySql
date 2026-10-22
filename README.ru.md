# NewLife.MySql — Чистый C# высокопроизводительный драйвер MySQL

[![AOT](https://github.com/NewLifeX/NewLife.MySql/actions/workflows/aot.yml/badge.svg?label=AOT)](https://github.com/NewLifeX/NewLife.MySql/actions/workflows/aot.yml)


![GitHub License](https://img.shields.io/github/license/NewLifeX/NewLife.MySql?logo=github)
![NuGet Downloads](https://img.shields.io/nuget/dt/NewLife.MySql?logo=nuget)
![NuGet](https://img.shields.io/nuget/v/NewLife.MySql?logo=nuget)
![NuGet prerelease](https://img.shields.io/nuget/vpre/NewLife.MySql?label=dev%20nuget&logo=nuget)
[![test](https://github.com/NewLifeX/NewLife.MySql/actions/workflows/test.yml/badge.svg?branch=master)](https://github.com/NewLifeX/NewLife.MySql/actions/workflows/test.yml)
[![locales](https://img.shields.io/badge/locales-11-2ea44f?logo=dotnet)](Doc/多语言本地化.md)
![platform](https://img.shields.io/badge/platform-Windows%20%7C%20Linux%20%7C%20macOS%20%7C%20Android%20%7C%20iOS-blue)
![arch](https://img.shields.io/badge/arch-x64%20%7C%20arm64%20%7C%20loongarch64%20%7C%20riscv64%20%7C%20s390x-orange)
[![contributors](https://img.shields.io/github/contributors/NewLifeX/NewLife.MySql?logo=github)](https://github.com/NewLifeX/NewLife.MySql/graphs/contributors)


<!-- lang:start -->
🌐 [English](README.md) | [简体中文](README.zh-CN.md) | [繁體中文](README.zh-TW.md) | [Español](README.es.md) | [Deutsch](README.de.md) | [日本語](README.ja.md) | [Français](README.fr.md) | [Português](README.pt.md) | [한국어](README.ko.md) | [العربية](README.ar.md)
<!-- lang:end -->

**NewLife.MySql** — это драйвер клиента MySQL на чистом C# от команды NewLife. Построенный на стандартных интерфейсах ADO.NET, он реализует протокольный уровень MySQL (Protocol Version 10) напрямую поверх TCP. Он зависит только от `NewLife.Core`, обеспечивая **нулевые сторонние зависимости**, **настоящую сквозную асинхронность** и **лицензию MIT**, чтобы коммерческое использование было беззаботным.

Его инновационное **конвейерное пакетное выполнение** (Pipeline) даёт ускорение **2×~3×** при пакетной записи в диапазоне десятков тысяч строк (по сравнению с построчной базовой линией), что делает драйвер идеальным выбором для массовых операций с большими данными и сценариев импортозамещения (信创).

---

## Цели дизайна

| Цель | Описание |
|------|------|
| **Лёгкость и минимализм** | Только стандартные для ADO.NET CRUD, транзакции, параметризованные запросы, хранимые процедуры и пакетные операции |
| **Высокая производительность** | Никаких лишних выделений памяти (`ArrayPool` / `OwnerPacket`), настоящий асинхронный ввод-вывод, конвейерное пакетное выполнение |
| **Полностью отечественный (китайский) стек** | Код ядра полностью контролируется самостоятельно, без рисков GPL-лицензирования, подходит для сред 信创 |
| **Дружелюбность к большим данным** | Словарные наборы параметров / привязка массивов / конвейеризация / многострочные VALUES / DbBatch — от десятков тысяч до миллионов строк |
| **Широкая совместимость** | `net45` ~ `net10` во всех версиях, MySQL 5.x ~ 9.0+, совместимость с OceanBase / TiDB / Doris / Aurora / CloudSQL, поддержка NativeAOT на `net8.0+` |

## Ключевые возможности

| Возможность | Описание |
|------|------|
| Управление соединениями | MySQL 5.x / 8.0 / 9.0+, несколько методов аутентификации: `mysql_native_password` + `caching_sha2_password` + `authentication_webauthn` |
| Unix Socket | Поддержка локальных подключений через Unix Domain Socket без накладных расходов TCP |
| Протокол сжатия | Поддержка сжатой передачи zlib (`UseCompression`) для снижения расхода сетевой пропускной способности; zstd отложен из-за требования нулевых сторонних зависимостей |
| Выполнение запросов | `ExecuteReader` / `ExecuteScalar` / `ExecuteNonQuery`, полный набор интерфейсов ADO.NET |
| Параметризованные запросы | Клиентская подстановка параметров с синтаксисом `@parameter-name` и автоматическим экранированием для защиты от инъекций |
| Подготовленные выражения | `Prepare()` использует бинарный протокол `COM_STMT_PREPARE`, есть необязательный глобальный переключатель `UseServerPrepare` |
| Пакетные операции | `ExecuteBatch` / `ExecuteArrayBatch` / конвейеризация / многострочные VALUES / DbBatch / `MySqlBulkCopy` |
| Конвейерное выполнение | `Pipeline=true` — пакетная отправка и пакетный приём, всего один сетевой round trip: 35× для тысяч строк и почти 100× для десятков тысяч строк |
| Транзакции | `BeginTransaction` / `Commit` / `Rollback`, четыре уровня изоляции, автоматический откат |
| Хранимые процедуры | `CommandType.StoredProcedure`, поддержка параметров IN/OUT |
| SSL/TLS | `SslMode=Preferred/Required`, поддержка TLS 1.2/1.3 |
| Пул соединений | Включён по умолчанию, отдельный пул для каждой строки подключения, настраиваемое время жизни и проверка простоя, автоматические проверки работоспособности и переработка соединений |
| Асинхронные методы | Полная цепочка `async/await`: `OpenAsync` / `ExecuteReaderAsync` / `ReadAsync` и т. д. |
| Запросы схемы | Метаданные Tables / Columns / Indexes / Databases, плюс расширенные запросы индексов и внешних ключей |
| DbDataSource | `MySqlDataSource` в стиле .NET 7+, поддерживает внедрение зависимостей |
| Трассировка | Встроенная инструментация `ActivitySource` (имя источника `NewLife.MySql`), создающая спаны Open / Execute / Commit / Rollback по стабильным семантическим соглашениям OpenTelemetry, без накладных расходов при отсутствии подписчиков |
| DbBatch | Стандартный пакетный API ADO.NET из .NET 6+ |
| Пространственные данные | Типы Geometry MySQL, оболочка WKB `MySqlGeometry` |
| EF Core | Поддерживается отдельным пакетом `NewLife.MySql.EntityFrameworkCore` |
| Интеграция с XCode | Автоматическая регистрация через `MySqlClientFactory`, бесшовная работа с ORM XCode |

---

## Ключевые преимущества

### 🚀 Конвейерное пакетное выполнение (Pipeline)

Инновационный конвейерный режим на основе бинарного протокола `COM_STMT_PREPARE + COM_STMT_EXECUTE`:

- **Этап 1**: собрать и отправить все пакеты EXECUTE сразу, выполняя flush только для последнего пакета
- **Этап 2**: последовательно прочитать все ответы OK/Error, накапливая число затронутых строк
- Стек TCP объединяет мелкие пакеты (алгоритм Нагла), поэтому получается всего один сетевой round trip

**Измеренные данные** (.NET 10 + MySQL 8.0.26 localhost, конвейеризация + транзакция против построчной базовой линии):

| Сценарий | Построчная базовая линия | Pipeline(tx) | Ускорение |
|------|------:|------:|------:|
| INSERT на 100 строк | 244ms | 20ms | 12.2× |
| INSERT на 1 000 строк | 2 473ms | 70ms | **35.3×** |
| INSERT на 10 000 строк | ~70s¹ | ~0.7s¹ | ~98× |

> ¹ Построчный показатель для 10 000 строк без транзакции экстраполирован (2.4ms на строку); показатель Pipeline измерен без транзакции. С транзакцией 10 000 строк занимают около 70ms.

**Данные по трём драйверам** (пакетные операции на 10 000 строк):

| Операция | NewLife Pipeline(tx) | MySql.Data Batch(tx) | MySqlConnector Batch(tx) |
|------|------:|------:|------:|
| INSERT | 899ms | 1 927ms | 1 906ms |
| UPDATE | 710ms | 2 265ms | 2 041ms |
| DELETE | 661ms | 1 961ms | 1 767ms |

> Уже на тысячах строк достигается ускорение **35×** в локальной сети; через центры обработки данных выигрыш ещё больше. Подробные данные см. в [Отчёте о тестировании производительности](Doc/性能测试报告.md).

### 🔧 Шесть вариантов пакетных операций

| Вариант | API | Сценарий применения |
|------|-----|---------|
| Наборы параметров в словаре | `ExecuteBatch` | Динамические параметры, задаваемые коллекцией |
| Привязка массива значений | `ExecuteBatchValues` | Массовый импорт (внутри `MySqlBulkCopy`), без выделения объектов словарей и параметров |
| Привязка массива | `ExecuteArrayBatch` | Массовые DML-операции в стиле Oracle |
| Конвейеризация | `Pipeline=true` | Десятки и сотни тысяч строк, межцентровый обмен |
| Многострочные VALUES | Конкатенация SQL | Чистые пакеты INSERT |
| DbBatch (.NET 6+) | `CreateBatch` | Смешанные пакеты разных SQL-выражений |

### 🛡️ Нулевые сторонние зависимости

Зависит только от `NewLife.Core` (тоже от команды NewLife), без рисков GPL-лицензирования, с лицензией MIT для беззаботного коммерческого использования; подходит для сред 信创 и сценариев со строгим аудитом безопасности.

### ⚡ Экстремальные оптимизации производительности

- `ArrayPool<T>` + `OwnerPacket` — никаких лишних выделений памяти
- Полностью пулированные `Pool.StringBuilder` / `Pool.MemoryStream`
- Полная цепочка `async/await`, без антипаттерна `sync-over-async`
- Компактный разбор протокола, читаются только необходимые поля

### 🌐 Широкая совместимость

- **Платформы**: `net45` / `net461` / `netstandard2.0` / `netstandard2.1` / `net6.0` / `net8.0` / `net10.0`
- **NativeAOT**: `net8.0+` поддерживает тримминг и NativeAOT (`IsAotCompatible`, есть smoke-тест `AotSmoke` и CI-матрица основных RID для защиты от регрессий; обязательны win-x64 / linux-x64 / linux-arm64; LoongArch linux-la64 покрывается эмуляцией QEMU (период наблюдения: запускается, сбои не блокируют)), см. [Оценку совместимости с AOT](Doc/AOT兼容性评估.md)
- **MySQL**: 5.5 ~ 9.0+, `mysql_native_password` + `caching_sha2_password`
- **Совместимые базы данных**: OceanBase, TiDB, Doris (автоопределение), Aurora / CloudSQL (облачные базы данных), MariaDB (базово)
- **Условная компиляция**: автоматический выбор оптимальной реализации для каждой платформы

---

## Сравнение возможностей

| Возможность | NewLife.MySql | MySqlConnector | MySql.Data (Oracle) |
|------|:---:|:---:|:---:|
| **Лицензия** | **MIT** | MIT | GPL (платно для коммерческого использования) |
| **Отечественная (китайская) автономность** | ✅ | ❌ | ❌ |
| **Сторонние зависимости** | **Нет** | Нет | Нет |
| **Настоящий асинхронный ввод-вывод** | ✅ | ✅ | ❌ |
| **Конвейерное пакетное выполнение** | ✅ | ❌ | ❌ |
| **Пакетная привязка массива** | ✅ | ❌ | ❌ |
| **Пакет с наборами параметров-словарей** | ✅ | ❌ | ❌ |
| **DbBatch (.NET 6+)** | ✅ | ✅ | ❌ |
| **DataAdapter** | ✅ | ❌ | ✅ |
| **EF Core** | ✅ | ✅ (Pomelo) | ✅ |
| **Интеграция с ORM XCode** | ✅ | ❌ | ❌ |
| **Целевые платформы** | net45~net10 | netstandard2.0+ | net462+ |
| **NativeAOT (net8.0+)** | ✅ | ✅ | ❌ |
| **Трассировка OpenTelemetry** | ✅ | ✅ | ✅ (8.1+) |
| **OceanBase / TiDB / Aurora / Doris** | ✅ Автоопределение | ❌ | ❌ |
| **MySqlBulkCopy** | ✅ | ✅ | ✅ |
| **Протокол сжатия (zlib)** | ✅ | ✅ | ❌ |
| **Unix Socket** | ✅ | ✅ | ❌ |
| **Аутентификация WebAuthn** | ✅ | ✅ | ❌ |
| **DbDataSource** | ✅ | ❌ | ❌ |

> Полный анализ возможностей и архитектуры см. в разделе [Архитектура](Doc/架构设计.md).

---

## Установка

```shell
dotnet add package NewLife.MySql
```

```xml
<PackageReference Include="NewLife.MySql" Version="1.0.*" />
```

---

## Быстрый старт

### Строка подключения

```
Server=localhost;Port=3306;Database=mydb;User Id=root;******;
```

| Параметр | Псевдонимы | По умолчанию | Описание |
|------|------|--------|------|
| Server | DataSource, Data Source | - | Адрес сервера |
| Port | - | 3306 | Номер порта |
| Database | - | - | Имя базы данных |
| UserID | Uid, User Id, User | - | Имя пользователя |
| Password | Pass, Pwd | - | Пароль |
| ConnectionTimeout | Connection Timeout | 15 | Тайм-аут подключения (секунды) |
| CommandTimeout | Default Command Timeout | 30 | Тайм-аут команды (секунды) |
| SslMode | Ssl Mode | None | Режим SSL (None/Preferred/Required) |
| UseServerPrepare | Use Server Prepare | false | Глобальный переключатель подготовки на сервере |
| Pipeline | Pipelining | false | Переключатель конвейерного выполнения |

### Открытие соединения

```csharp
using var conn = new MySqlConnection("Server=localhost;Database=mydb;User Id=root;******;");
conn.Open();
// The connection is closed automatically and returned to the pool when the using scope ends
```

### Запрос набора результатов

```csharp
using var conn = new MySqlConnection(connStr);
conn.Open();

using var cmd = new MySqlCommand(conn, "SELECT id, name, age FROM users WHERE age > 18");
using var reader = cmd.ExecuteReader();

while (reader.Read())
{
    var id = reader.GetInt64(0);
    var name = reader.GetString(1);
    var age = reader.GetInt64(2);
    Console.WriteLine($"{id}: {name}, {age}");
}
```

### Запрос скалярного значения

```csharp
using var cmd = new MySqlCommand(conn, "SELECT COUNT(*) FROM users");
var count = cmd.ExecuteScalar();
```

### Выполнение DML

```csharp
using var cmd = new MySqlCommand(conn, "INSERT INTO users(name, age) VALUES('Tom', 25)");
var affectedRows = cmd.ExecuteNonQuery();

// Shortcut
var rows = conn.ExecuteNonQuery("DELETE FROM logs WHERE created < '2024-01-01'");
```

---

## Параметризованные запросы

Используйте синтаксис `@parameter-name` для привязки параметров; значения экранируются автоматически, что предотвращает инъекции:

```csharp
using var cmd = new MySqlCommand(conn, "SELECT * FROM users WHERE name = @name AND age > @age");
cmd.Parameters.AddWithValue("name", "Tom");
cmd.Parameters.AddWithValue("age", 18);

using var reader = cmd.ExecuteReader();
while (reader.Read())
{
    Console.WriteLine(reader.GetString(0));
}
```

```csharp
// Parameterized insert
using var cmd = new MySqlCommand(conn, "INSERT INTO users(name, age, created) VALUES(@name, @age, @dt)");
cmd.Parameters.AddWithValue("name", "Alice");
cmd.Parameters.AddWithValue("age", 30);
cmd.Parameters.AddWithValue("dt", DateTime.Now);
cmd.ExecuteNonQuery();
```

```csharp
// Passing NULL values
cmd.Parameters.AddWithValue("email", DBNull.Value);
```

**Поддерживаемые типы параметров**:

| Тип .NET | Пример SQL-литерала |
|-----------|---------------|
| `String` | `'hello'` (специальные символы экранируются автоматически) |
| `Int32` / `Int64` и другие числовые типы | `42` |
| `Boolean` | `1` или `0` |
| `DateTime` | `'2025-07-01 12:30:00'` |
| `Byte[]` | `X'CAFE'` |
| `MySqlGeometry` | Геометрические данные в формате WKB |
| `Guid` | `'01234567-89ab-cdef-...'` |
| `Enum` | Преобразуется в число |
| `null` / `DBNull.Value` | `NULL` |

---

## Транзакции

```csharp
using var conn = new MySqlConnection(connStr);
conn.Open();

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

Поддерживаются четыре уровня изоляции: `ReadUncommitted` / `ReadCommitted` / `RepeatableRead` (по умолчанию) / `Serializable`

```csharp
using var tr = conn.BeginTransaction(IsolationLevel.ReadCommitted);
```

Если транзакция освобождается без фиксации или отката, откат выполняется автоматически.

---

## Хранимые процедуры

```csharp
using var cmd = new MySqlCommand { Connection = conn, CommandType = CommandType.StoredProcedure };
cmd.CommandText = "my_proc";
cmd.Parameters.AddWithValue("p_id", 1);

var outParam = new MySqlParameter { ParameterName = "p_result", Direction = ParameterDirection.Output };
cmd.Parameters.Add(outParam);

cmd.ExecuteNonQuery();
var result = outParam.Value; // Output parameter value
```

---

## Подготовленные выражения

Используйте `Prepare()` для компиляции SQL на сервере; последующие выполнения идут по бинарному протоколу:

```csharp
using var cmd = new MySqlCommand(conn, "INSERT INTO users(name, age) VALUES(@name, @age)");
cmd.Parameters.AddWithValue("name", "Tom");
cmd.Parameters.AddWithValue("age", 25);

cmd.Prepare(); // Server-side prepare

cmd.ExecuteNonQuery(); // First execution

// Execute again after changing parameters (no recompilation needed)
cmd.Parameters[0].Value = "Jerry";
cmd.Parameters[1].Value = 30;
cmd.ExecuteNonQuery();
```

Включить это глобально можно через строку подключения, тогда каждый параметризованный запрос автоматически использует бинарный протокол:

```
UseServerPrepare=true;
```

---

## Пакетные операции

### Вариант A: наборы параметров в словаре

Выполнение одного и того же SQL с несколькими наборами привязанных параметров; подготовка внутри выполняется автоматически:

```csharp
using var cmd = new MySqlCommand(conn, "INSERT INTO users(name, age) VALUES(@name, @age)");
cmd.Parameters.AddWithValue("name", "");
cmd.Parameters.AddWithValue("age", 0);

var paramSets = new List<IDictionary<String, Object?>>
{
    new Dictionary<String, Object?> { ["name"] = "Alice", ["age"] = 25 },
    new Dictionary<String, Object?> { ["name"] = "Bob", ["age"] = 30 },
    new Dictionary<String, Object?> { ["name"] = "Charlie", ["age"] = 22 },
};
var totalAffected = cmd.ExecuteBatch(paramSets);
// or await cmd.ExecuteBatchAsync(paramSets);
```

### Вариант B: привязка массивов (в стиле Oracle)

Задайте значения параметров массивами и укажите количество выполнений:

```csharp
using var cmd = new MySqlCommand(conn, "INSERT INTO users(name, age) VALUES(@name, @age)");
cmd.Parameters.AddWithValue("name", new[] { "Alice", "Bob", "Charlie" });
cmd.Parameters.AddWithValue("age", new[] { 25, 30, 22 });

var totalAffected = cmd.ExecuteArrayBatch(3);
// or await cmd.ExecuteArrayBatchAsync(3);
```

### Вариант C: конвейерное выполнение

Включите `Pipeline=true` в строке подключения, чтобы отправлять и принимать данные пакетами всего за один сетевой round trip:

```csharp
var connStr = "Server=localhost;Database=mydb;User Id=root;******;Pipeline=true;";
using var conn = new MySqlConnection(connStr);
conn.Open();

using var cmd = new MySqlCommand(conn, "UPDATE users SET age=@age WHERE name=@name");
cmd.Parameters.AddWithValue("age", agesArray);    // Int32[10000]
cmd.Parameters.AddWithValue("name", namesArray);   // String[10000]
var totalAffected = cmd.ExecuteArrayBatch(10000);
```

Конвейеризация работает для INSERT / UPDATE / DELETE и может сочетаться со словарными наборами параметров или привязкой массивов. Чем выше сетевая задержка (например, между центрами обработки данных), тем больше выигрыш.

### Вариант D: многострочные VALUES

```csharp
var sql = "INSERT INTO users(name, age) VALUES('Alice', 25), ('Bob', 30), ('Charlie', 22)";
conn.ExecuteNonQuery(sql);
```

### Вариант E: API DbBatch (.NET 6+)

```csharp
var batch = conn.CreateBatch();
batch.BatchCommands.Add(new MySqlBatchCommand("INSERT INTO users(name, age) VALUES('X1', 10)"));
batch.BatchCommands.Add(new MySqlBatchCommand("INSERT INTO users(name, age) VALUES('X2', 20)"));
batch.BatchCommands.Add(new MySqlBatchCommand("UPDATE users SET age = age + 1 WHERE name = 'X1'"));

using var reader = batch.ExecuteReader();
```

---

## Поддержка EF Core

Пакет `NewLife.MySql.EntityFrameworkCore` предоставляет провайдер базы данных EF Core с поддержкой .NET 6 / .NET 8 / .NET 10.

```shell
dotnet add package NewLife.MySql.EntityFrameworkCore
```

```csharp
public class AppDbContext : DbContext
{
    public DbSet<User> Users { get; set; } = null!;

    public AppDbContext(DbContextOptions<AppDbContext> options) : base(options) { }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<User>(entity =>
        {
            entity.ToTable("users");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Name).HasMaxLength(100).IsRequired();
        });
    }
}

var options = new DbContextOptionsBuilder<AppDbContext>()
    .UseMySql("Server=localhost;Database=mydb;User Id=root;******;")
    .Options;

using var context = new AppDbContext(options);
```

**Поддерживаемые возможности**:

| Возможность | Описание |
|------|------|
| Сопоставление типов | Int / Long / String / DateTime / Decimal / Guid / Boolean / Byte[] |
| Трансляция запросов | LINQ → синтаксис MySQL (обратные кавычки, LIMIT/OFFSET) |
| Трансляция методов | Contains / StartsWith / EndsWith / ToUpper / ToLower / Trim / Replace / Substring / Length |
| Трансляция членов | DateTime.Year / Month / Day / Hour / Minute / Second |
| Поддержка миграций | CreateTable / AlterColumn / AddColumn / DropColumn / RenameTable / CreateIndex / DropIndex |
| Стратегия генерации значений | Первичные ключи-идентификаторы AUTO_INCREMENT |

---

## Шифрованные соединения SSL/TLS

```csharp
// Encrypt if the server supports it, otherwise use plaintext
var connStr = "Server=localhost;Database=mydb;User Id=root;******;SslMode=Preferred;";

// Require encryption; throw if the server does not support it
var connStr2 = "Server=localhost;Database=mydb;User Id=root;******;SslMode=Required;";
```

| SslMode | Поведение |
|---------|------|
| `None` / `Disabled` | Не использовать SSL (по умолчанию) |
| `Preferred` | Предпочитать шифрование, при отсутствии поддержки переключаться на открытый текст |
| `Required` | Требовать шифрование |

---

## Запросы метаданных схемы

```csharp
var databases = conn.GetSchema("Databases");  // Database list
var tables = conn.GetSchema("Tables");         // Table list
var columns = conn.GetSchema("Columns");       // Column information
var indexes = conn.GetSchema("Indexes");       // Index information
```

---

## Асинхронные методы

Все основные операции поддерживают асинхронные версии:

```csharp
using var conn = new MySqlConnection(connStr);
await conn.OpenAsync();

using var cmd = new MySqlCommand(conn, "SELECT * FROM users");
using var reader = await cmd.ExecuteReaderAsync();

while (await reader.ReadAsync())
{
    Console.WriteLine(reader.GetString(0));
}
```

---

## Трассировка (OpenTelemetry)

В драйвер встроена инструментация `ActivitySource` (имя источника `NewLife.MySql`), создающая спаны четырёх видов: `Open` (соединение), `Execute` (команда, охватывающая время жизни reader), `Commit` / `Rollback` (транзакции). Атрибуты соответствуют стабильным семантическим соглашениям OpenTelemetry для баз данных (`db.system.name` / `db.query.text` / `server.address` / `error.type` и т. д.). OpenTelemetry SDK предоставляет потребитель; драйвер не добавляет зависимостей:

```csharp
using var tracerProvider = Sdk.CreateTracerProviderBuilder()
    .AddSource("NewLife.MySql")
    .AddOtlpExporter()
    .Build();
```

- **Область действия**: `net6.0` и выше; на более низких версиях и при отсутствии подписчиков — no-op без накладных расходов
- **Замечание о приватности**: `db.query.text` записывает текст команды; предпочитайте параметризованные запросы, чтобы литералы не попадали в телеметрию

---

## Пул соединений

Пул соединений включён по умолчанию и не требует дополнительной настройки. Каждая уникальная строка подключения соответствует своему независимому пулу.

- Соединения берутся из пула автоматически при открытии
- Соединения возвращаются автоматически при закрытии (TCP-соединение не разрывается)
- Некорректные соединения удаляются автоматически
- Новые соединения создаются по требованию
- **Автоматическое переподключение после обрыва сети**: при обнаружении разорванного соединения выполняется автоматический повтор, что повышает стабильность в долгоживущих сценариях

### Параметры пула соединений

| Параметр | По умолчанию | Описание |
|------|--------|------|
| MinPoolSize | 0 | Минимальное количество соединений |
| MaxPoolSize | 100 | Максимальное количество соединений |
| ConnectionLifeTime | 0 | Время жизни соединения (секунды); 0 означает, что срок не истекает |
| ConnectionIdleTime | 300 | Тайм-аут простоя (секунды); по его истечении соединения изымаются |
| ValidationInterval | 30 | Интервал проверки простоя (секунды) |

```csharp
var connStr = "Server=localhost;Database=mydb;...;MinPoolSize=5;MaxPoolSize=50;ConnectionLifeTime=3600;";
```

---

## Интеграция с ORM XCode

Автоматически регистрируется как драйвер MySQL для XCode через `MySqlClientFactory`:

```csharp
using XCode.DataAccessLayer;

DAL.AddConnStr("mysql", "Server=localhost;Database=mydb;User Id=root;******;", null, "MySql");

var dal = DAL.Create("mysql");
var tables = dal.Tables;
```

---

## Переключение баз данных

### ChangeDatabase (рекомендуется)

```csharp
using var conn = new MySqlConnection("Server=localhost;Database=db1;...");
conn.Open();
conn.ChangeDatabase("db2"); // Internally Close + Reopen
```

> ⚠️ Переключение посреди транзакции не поддерживается (транзакция теряется); создавайте отдельное соединение для каждой базы данных.

### SqlClient.SetDatabaseAsync (низкоуровневый API)

```csharp
await conn.Client.SetDatabaseAsync("information_schema"); // COM_INIT_DB
```

Соединение не закрывается; переключается только текущая база данных на сервере, что подходит для временных сценариев запросов.

---

## Поддержка OceanBase / TiDB

Драйвер автоматически определяет тип базы данных по строке версии в пакете рукопожатия; способ подключения полностью совпадает с MySQL:

```csharp
// OceanBase
var connStr = "Server=oceanbase-host;Port=2881;Database=test;User Id=root;******;";
using var conn = new MySqlConnection(connStr);
conn.Open();
Console.WriteLine($"Database type: {conn.DatabaseType}");  // OceanBase

// TiDB
var connStr2 = "Server=tidb-host;Port=4000;Database=test;User Id=root;******;";
using var conn2 = new MySqlConnection(connStr2);
conn2.Open();
Console.WriteLine($"Database type: {conn2.DatabaseType}");  // TiDB
```

Базовый SQL, транзакции, параметризованные запросы и пакетные операции полностью совместимы.

---

## Целевые платформы

| Платформа | Описание |
|------|------|
| `net45` / `net461` | .NET Framework, совместимость с унаследованными проектами |
| `netstandard2.0` / `netstandard2.1` | Кроссплатформенность, поддержка асинхронного Dispose |
| `net6.0` | Дополнительно поддерживает API DbBatch |
| `net10.0` | Новейший .NET, оптимальная производительность |

---

## Развёртывание NativeAOT

На целевых платформах `net8.0+` драйвер прошёл реальную проверку публикации NativeAOT (локальные измерения и CI на win-x64 и linux-x64; все 26 smoke-сценариев пройдены с 0 предупреждений ILC). Публикация нативных бинарников **не требует патчей на стороне вызывающего кода**; CI непрерывно защищает от регрессий по матрице основных RID (win-x64 / linux-x64 / linux-arm64 / win-arm64 / osx-x64 / osx-arm64 / musl, а также smoke-тест LoongArch linux-la64 под эмуляцией QEMU в период наблюдения, который не блокирует сборку):

```xml
<PropertyGroup>
  <PublishAot>true</PublishAot>
</PropertyGroup>
```

```powershell
dotnet publish -c Release -r linux-x64
```

| Пункт | Содержание |
|------|------|
| Область | `net8.0+`; для `net6.0` требуется экспериментальный переключатель, более ранние платформы не поддерживаются |
| Проверенные возможности | Подключение и аутентификация (включая атрибуты соединения при рукопожатии), синхронные и асинхронные запросы, подготовка на сервере, пакетные операции (словарь/массив/массив значений/конвейер), транзакции, хранимые процедуры, метаданные схемы, DbDataSource, DataAdapter, MySqlBulkCopy, протокол сжатия, подписка на binlog, трассировка |
| Примечания | Семейство `MySqlDataAdapter` Fill/FillSchema/Update снабжено аннотациями `RequiresUnreferencedCode`; приложения с триммингом объявляют ответственность за сохранение кода в месте вызова согласно семантике System.Data |
| Подробная оценка | [Оценка совместимости с AOT](Doc/AOT兼容性评估.md) |

---

## Примечания

1. **Версия MySQL**: поддерживаются 5.x и выше, рекомендуется 8.0+
2. **Кодировка символов**: по умолчанию UTF-8
3. **ChangeDatabase**: реализован через Close/Reopen; переключение посреди транзакции не поддерживается, используйте отдельное соединение
4. **Конвейерное выполнение**: только для пакетных DML-операций, не для запросов SELECT

---

## Связанная документация

| Документ | Описание |
|------|------|
| [Требования](Doc/需求文档.md) | Видение, ключевые цели, направления возможностей |
| [Список функций](Doc/功能清单.md) | Трёхмерное отслеживание статуса реализации/тестирования/комментариев |
| [Архитектура](Doc/架构设计.md) | Обзор архитектуры, реализация протокола, выбор технологий, проектные решения, сравнение драйверов |
| [Отчёт о тестировании производительности](Doc/性能测试报告.md) | Подробные данные бенчмарков пакетных операций |
| [Журнал изменений (中文)](Doc/ChangeLog.md) | Китайский журнал релизов |
| [Журнал изменений (EN)](Doc/ChangeLog.en.md) | Английский журнал релизов |
| [README на упрощённом китайском](README.zh-CN.md) | Китайская документация, поддерживаемая в том же выпуске, что и эта английская версия |
| [Руководство по миграции: MySql.Data → NL](Doc/迁移指南_MySqlData.md) | Шаги и замечания по миграции с официального драйвера Oracle |
| [Руководство по миграции: MySqlConnector → NL](Doc/迁移指南_MySqlConnector.md) | Шаги и замечания по миграции с популярного community-драйвера |

## Карта экосистемы

NewLife — это многоуровневая экосистема независимо версионируемых open-source проектов. Карта ниже дословно используется во всех репозиториях NewLife; каждый проект ссылается на свой репозиторий и свой основной пакет NuGet.

| Проект | Что делает | Загрузки |
| --- | --- | --- |
| **Базовый** | | |
| [NewLife.Core](https://github.com/NewLifeX/X) | Логирование, конфигурация, кэширование, сеть, сериализация, планирование и APM-трассировка. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Core?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Core) |
| [NewLife.XCode](https://github.com/NewLifeX/NewLife.XCode) | Промежуточное ПО данных: отдельные таблицы масштаба десятков миллиардов строк, автоматическое секционирование, разделение чтения и записи, более 10 баз данных. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.XCode?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.XCode) |
| [NewLife.Net](https://github.com/NewLifeX/NewLife.Net) | Высокопроизводительная сетевая библиотека: 1,77M pps / 14,5 Гбит/с на машину (TCP 1KB, 2026-10), 4M долгоживущих TCP-соединений. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Net?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Net) |
| [NewLife.Remoting](https://github.com/NewLifeX/NewLife.Remoting) | Протокольная связь и RPC-фреймворк для клиент-серверных приложений и IoT-устройств. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Remoting?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Remoting) |
| [NewLife.Agent](https://github.com/NewLifeX/NewLife.Agent) | Устанавливает .NET-приложения как демоны ОС: службы Windows и systemd в Linux. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Agent?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Agent) |
| [NewLife.Templates](https://github.com/NewLifeX/NewLife.Zero) | Шаблоны проектов `dotnet new` для консоли, web, API, службы, данных и IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Templates?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Templates) |
| **Данные и обмен сообщениями** | | |
| [NewLife.Redis](https://github.com/NewLifeX/NewLife.Redis) | Клиент Redis с микросекундной задержкой, миллионами операций в секунду и встроенными очередями сообщений. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Redis?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Redis) |
| [NewLife.RocketMQ](https://github.com/NewLifeX/NewLife.RocketMQ) | Полностью управляемый клиент RocketMQ для Apache RocketMQ и Alibaba Cloud Message Queue. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.RocketMQ?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.RocketMQ) |
| [NewLife.MQTT](https://github.com/NewLifeX/NewLife.MQTT) | Клиент и сервер MQTT, включая подключение к Alibaba Cloud IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.MQTT?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.MQTT) |
| [NewLife.NovaDb](https://github.com/NewLifeX/NewLife.NovaDb) | Гибридная база данных на C#: встроенная или сетевая, реляционная, временных рядов, очередь сообщений и KV в одном. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.NovaDb?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.NovaDb) |
| [NewLife.MySql](https://github.com/NewLifeX/NewLife.MySql) | Драйвер MySql и вспомогательный слой на основе стека данных NewLife. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.MySql?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.MySql) |
| **Web, AI и распределённые системы** | | |
| [NewLife.Cube](https://github.com/NewLifeX/NewLife.Cube) | Фреймворк быстрой разработки админ-платформ: права, SSO, сервер OAuth и CRUD-скаффолдинг. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Cube?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Cube) |
| [Stardust](https://github.com/NewLifeX/Stardust) | Распределённая сервисная платформа: управление узлами, APM, центр конфигурации, реестр и центр публикации. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Stardust?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Stardust) |
| [AntJob](https://github.com/NewLifeX/AntJob) | Распределённая платформа задач и планирования данных в реальном времени с секционированием, повторами и дозаполнением. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.AntJob?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.AntJob) |
| [NewLife.AI](https://github.com/NewLifeX/NewLife.AI) | Единый клиент для 46 поставщиков LLM плюс ChatAI — готовое веб-приложение для чата. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.AI?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.AI) |
| **Промышленность и IoT** | | |
| [NewLife.IoT](https://github.com/NewLifeX/NewLife.IoT) | Стандартная библиотека IoT, определяющая контракты протоколов и абстракции устройств. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.IoT?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.IoT) |
| [NewLife.Modbus](https://github.com/NewLifeX/NewLife.Modbus) | Реализация Modbus TCP/RTU/ASCII поверх NewLife.IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Modbus?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Modbus) |
| [NewLife.Siemens](https://github.com/NewLifeX/NewLife.Siemens) | Протокол Siemens PLC поверх NewLife.IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Siemens?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Siemens) |
| [NewLife.Omron](https://github.com/NewLifeX/NewLife.Omron) | Протокол Omron PLC поверх NewLife.IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Omron?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Omron) |
| [NewLife.Melsec](https://github.com/NewLifeX/NewLife.Melsec) | Протокол Mitsubishi Melsec PLC поверх NewLife.IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Melsec?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Melsec) |
| [NewLife.OPC](https://github.com/NewLifeX/NewLife.OPC) | Клиент OPC для доступа к промышленным данным. | — |
| [NewLife.BACnet](https://github.com/NewLifeX/NewLife.BACnet) | Стек протокола автоматизации зданий BACnet. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.BACnet?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.BACnet) |
| [ZeroIoT](https://github.com/NewLifeX/ZeroIoT) | Лёгкая open-source платформа IoT с управлением шлюзами и устройствами. | — |
| **Телематика транспорта** | | |
| [NewLife.JT808](https://github.com/NewLifeX/NewLife.JT808) | Протокол позиционирования транспорта JT/T 808: более 70 типов сообщений, 2011/2013/2019. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT808?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT808) |
| [NewLife.JT809](https://github.com/NewLifeX/NewLife.JT809) | Межплатформенный протокол JT/T 809 для органов управления транспортом. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT809?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT809) |
| [NewLife.JT1078](https://github.com/NewLifeX/NewLife.JT1078) | Автомобильное видео JT/T 1078: разбор RTP, ремультиплексирование в FLV/fMP4 и прямая трансляция. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT1078?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT1078) |
| **Прикладные компоненты** | | |
| [NewLife.Office](https://github.com/NewLifeX/NewLife.Office) | Один пакет для Excel, Word, PPT, PDF и более 15 форматов документов. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Office?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Office) |
| [NewLife.Audio](https://github.com/NewLifeX/NewLife.Audio) | Кроссплатформенное аудио: кодеки, ввод-вывод устройств, DSP, контейнеры и потоковая передача. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Audio?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Audio) |
| [NewLife.Video](https://github.com/NewLifeX/NewLife.Video) | Видеокодеки, форматы контейнеров, обработка кадров и потоковая передача. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Video?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Video) |
| [NewLife.Map](https://github.com/NewLifeX/NewLife.Map) | Единый интерфейс к Baidu, AMap, Tencent и Tianditu с геокодированием и преобразованием координат. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Map?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Map) |
| [NewLife.IP](https://github.com/NewLifeX/NewLife.IP) | Геолокация IPv4 с офлайн-базой, отображаемой в память, и онлайн-API как резервом. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.IP?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.IP) |
| **Инструменты и продукты** | | |
| [CrazyCoder](https://github.com/NewLifeX/XCoder) | Инструменты разработчика: сеть, последовательный порт, шифрование, регулярные выражения, Modbus и MQTT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.XCoder?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.XCoder) |
| [GitCandy](https://github.com/NewLifeX/GitCandy) | Самостоятельно размещаемая система управления исходным кодом Git. | — |
| [LuckyClover](https://github.com/NewLifeX/LuckyClover) | Автоматически устанавливает подходящую среду выполнения .NET при запуске на старом хосте. | — |

> Бейджи показывают основной пакет каждого проекта; пакеты тем, расширений и сателлитов перечислены на соответствующих страницах NuGet. Более 30 пакетов, не перечисленных здесь (FTP, LoRa, расширения Office, драйверы протоколов и другие), доступны в [организации NewLifeX](https://github.com/NewLifeX) и на [newlifex.com](https://newlifex.com).

## Команда

<img src="https://newlifex.com/logo.png" width="48" align="right" alt="NewLife" />

Команда NewLife основана в 2002 году и более двух десятилетий работает в экосистеме .NET и IoT, поддерживает около 100 проектов с открытым кодом (34 из них — основные) и предлагает интегрированные программно-аппаратные решения для энергетики, логистики, здравоохранения, промышленной автоматизации и интернет-отрасли. Мы следуем девизу «в учении нет старших — мастерство приносит уважение».

Open-source компоненты скачали <!--nuget-total:start-->6.48M<!--nuget-total:end--> раз с NuGet (по всей экосистеме, по состоянию на 2026-10).

Сайт: https://newlifex.com · Организация open source: https://github.com/NewLifeX

<details>
<summary>Официальный аккаунт WeChat: 智能大石头 (отсканируйте, чтобы подписаться)</summary>

![智能大石头](https://newlifex.com/stone.jpg)

</details>
