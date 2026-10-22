# NewLife.MySql — برنامج تشغيل MySQL عالي الأداء بلغة C# خالصة

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
🌐 [简体中文](README.zh-CN.md) | [繁體中文](README.zh-TW.md) | [Español](README.es.md) | [Deutsch](README.de.md) | [日本語](README.ja.md) | [Français](README.fr.md) | [Português](README.pt.md) | [Русский](README.ru.md) | [한국어](README.ko.md) | [العربية](README.ar.md)
<!-- lang:end -->

**NewLife.MySql** برنامج تشغيل عميل MySQL مكتوب بلغة C# خالصة من فريق NewLife. بُني على واجهات ADO.NET القياسية، ويُنفّذ طبقة بروتوكول MySQL (إصدار البروتوكول 10) مباشرةً فوق TCP. يعتمد على `NewLife.Core` فقط، بـ**صفر تبعيات خارجية**، و**غير متزامن حقيقي من الطرف إلى الطرف**، و**ترخيص MIT** يتيح الاستخدام التجاري دون قلق.

توفّر ميزة **التنفيذ الدفعي المُتدفّق** (Pipeline) المبتكرة تسريعًا بمقدار **2×~3×** للكتابة الدفعية في نطاق عشرات الآلاف من الصفوف (مقارنةً بالأساس صف-بصف)، ما يجعلها الخيار الأمثل لعمليات البيانات الضخمة وسيناريوهات الاستبدال المحلي (信创).

---

## أهداف التصميم

| الهدف | الوصف |
|------|------|
| **خفيف وبسيط** | فقط CRUD القياسي في ADO.NET، والمعاملات، والاستعلامات ذات المعاملات، والإجراءات المخزّنة، والعمليات الدفعية |
| **أداء عالٍ** | صفر تخصيصات ذاكرة إضافية (`ArrayPool` / `OwnerPacket`)، وإدخال/إخراج غير متزامن حقيقي، وتنفيذ دفعي مُتدفّق |
| **مكدّس محلي (صيني) خالص** | الشيفرة الأساسية مملوكة بالكامل دون مخاطر ترخيص GPL، ومناسبة لبيئات 信创 |
| **صديق للبيانات الضخمة** | مجموعات معاملات قاموسية / ربط مصفوفات / تنفيذ مُتدفّق / VALUES متعدد الصفوف / DbBatch، ويغطّي من عشرات الآلاف إلى ملايين الصفوف |
| **توافق واسع** | `net45` ~ `net10` عبر جميع الإصدارات، وMySQL 5.x ~ 9.0+، ومتوافق مع OceanBase / TiDB / Doris / Aurora / CloudSQL، ويدعم NativeAOT على `net8.0+` |

## الميزات الأساسية

| الميزة | الوصف |
|------|------|
| إدارة الاتصالات | MySQL 5.x / 8.0 / 9.0+، وطرق مصادقة متعددة: `mysql_native_password` + `caching_sha2_password` + `authentication_webauthn` |
| Unix Socket | يدعم الاتصالات المحلية عبر Unix Domain Socket دون أي كلفة TCP |
| بروتوكول الضغط | يدعم النقل المضغوط zlib (`UseCompression`) لتقليل استهلاك عرض النطاق؛ وتأجيل zstd بسبب قيد صفر التبعيات الخارجية |
| تنفيذ الاستعلامات | `ExecuteReader` / `ExecuteScalar` / `ExecuteNonQuery`، وواجهات ADO.NET كاملة |
| الاستعلامات ذات المعاملات | استبدال المعاملات من جانب العميل بصيغة `@parameter-name` مع تهريب تلقائي لمنع الحقن |
| العبارات المُعدّة مسبقًا | يستخدم `Prepare()` البروتوكول الثنائي `COM_STMT_PREPARE`، مع مفتاح عام اختياري `UseServerPrepare` |
| العمليات الدفعية | `ExecuteBatch` / `ExecuteArrayBatch` / التنفيذ المُتدفّق / VALUES متعدد الصفوف / DbBatch / `MySqlBulkCopy` |
| التنفيذ المُتدفّق | `Pipeline=true` يرسل دفعةً ويستقبل دفعةً، برحلة شبكة واحدة فقط: 35× لبضعة آلاف من الصفوف وما يقارب 100× لعشرات الآلاف |
| المعاملات | `BeginTransaction` / `Commit` / `Rollback`، وأربعة مستويات عزل، وإلغاء تلقائي |
| الإجراءات المخزّنة | `CommandType.StoredProcedure`، وتدعم معاملات IN/OUT |
| SSL/TLS | `SslMode=Preferred/Required`، ويدعم TLS 1.2/1.3 |
| تجمّع الاتصالات | مُفعّل افتراضيًا، ويُجمَّع لكل سلسلة اتصال، مع عمر قابل للضبط وتحقق من الخمول، وفحوص صحة وإعادة تدوير تلقائية |
| الطرق غير المتزامنة | `async/await` على امتداد السلسلة: `OpenAsync` / `ExecuteReaderAsync` / `ReadAsync` وغيرها |
| استعلامات المخطط | بيانات وصفية مثل Tables / Columns / Indexes / Databases، إضافةً إلى استعلامات موسّعة للفهارس والمفاتيح الخارجية |
| DbDataSource | `MySqlDataSource` بأسلوب .NET 7+، ويدعم حقن التبعيات |
| التتبّع | أداة `ActivitySource` مدمجة (اسم المصدر `NewLife.MySql`)، تُصدر مقاطع Open / Execute / Commit / Rollback وفق اصطلاحات OpenTelemetry المستقرة، وبصفر كلفة عند عدم وجود مشتركين |
| DbBatch | واجهة الدفعات القياسية في ADO.NET منذ .NET 6+ |
| البيانات المكانية | أنواع Geometry في MySQL، وغلّاف `MySqlGeometry` بصيغة WKB |
| EF Core | مدعوم عبر حزمة `NewLife.MySql.EntityFrameworkCore` المستقلة |
| دمج XCode | تسجيل تلقائي عبر `MySqlClientFactory`، وتكامل سلس مع ORM الخاص بـXCode |

---

## أبرز المزايا

### 🚀 التنفيذ الدفعي المُتدفّق (Pipeline)

وضع مُتدفّق مبتكر يقوم على البروتوكول الثنائي `COM_STMT_PREPARE + COM_STMT_EXECUTE`:

- **المرحلة الأولى**: بناء وإرسال جميع حزم EXECUTE دفعةً واحدة، مع تفريغ الحزمة الأخيرة فقط
- **المرحلة الثانية**: قراءة جميع استجابات OK/Error بالترتيب دفعةً واحدة، مع تجميع عدد الصفوف المتأثرة
- تُدمج حزمة TCP الحزم الصغيرة (خوارزمية Nagle)، لذا لا تحدث سوى رحلة شبكة واحدة

**بيانات مقاسة** (.NET 10 + MySQL 8.0.26 على localhost، التنفيذ المُتدفّق + معاملة مقابل الأساس صف-بصف):

| السيناريو | الأساس صف-بصف | Pipeline(tx) | التسريع |
|------|------:|------:|------:|
| INSERT لـ100 صف | 244ms | 20ms | 12.2× |
| INSERT لـ1,000 صف | 2,473ms | 70ms | **35.3×** |
| INSERT لـ10,000 صف | ~70s¹ | ~0.7s¹ | ~98× |

> ¹ رقم 10,000 صف بأسلوب صف-بصف دون معاملة مستنتج (2.4ms لكل صف)؛ ورقم Pipeline مقاس دون معاملة. ومع معاملة، تستغرق 10,000 صف نحو 70ms.

**بيانات أداء برامج التشغيل الثلاثة** (عمليات دفعية من 10,000 صف):

| العملية | NewLife Pipeline(tx) | MySql.Data Batch(tx) | MySqlConnector Batch(tx) |
|------|------:|------:|------:|
| INSERT | 899ms | 1,927ms | 1,906ms |
| UPDATE | 710ms | 2,265ms | 2,041ms |
| DELETE | 661ms | 1,961ms | 1,767ms |

> تحقّق بضعة آلاف من الصفوف تسريعًا بمقدار **35×** على شبكة محلية؛ وتزيد الفائدة عبر مراكز البيانات. راجع [تقرير اختبار الأداء](Doc/性能测试报告.md) للبيانات التفصيلية.

### 🔧 ستة خيارات للعمليات الدفعية

| الخيار | واجهة API | حالة الاستخدام |
|------|-----|---------|
| مجموعات معاملات قاموسية | `ExecuteBatch` | معاملات ديناميكية تُقدَّم من مجموعة |
| ربط مصفوفة قيم | `ExecuteBatchValues` | استيراد ضخم (داخل `MySqlBulkCopy`)، بصفر تخصيصات لكائنات القاموس/المعاملات |
| ربط المصفوفات | `ExecuteArrayBatch` | DML بأحجام كبيرة، بأسلوب Oracle |
| التنفيذ المُتدفّق | `Pipeline=true` | عشرات الآلاف / مئات الآلاف من الصفوف، وعبر مراكز البيانات |
| VALUES متعدد الصفوف | SQL مُسلسَل | دفعات INSERT خالصة |
| DbBatch (.NET 6+) | `CreateBatch` | دفعات مختلطة من عبارات SQL مختلفة |

### 🛡️ صفر تبعيات خارجية

يعتمد على `NewLife.Core` فقط (وهو أيضًا من فريق NewLife)، دون أي مخاطر ترخيص GPL، وبرخصة MIT تتيح الاستخدام التجاري دون قلق، مع ملاءمته لبيئات 信创 والسيناريوهات ذات متطلبات تدقيق أمني صارمة.

### ⚡ تحسينات أداء فائقة

- `ArrayPool<T>` + `OwnerPacket` لصفر تخصيصات إضافية
- `Pool.StringBuilder` / `Pool.MemoryStream` مُجمّعة بالكامل
- `async/await` على امتداد السلسلة، دون نمط `sync-over-async` العكسي
- تحليل بروتوكول مُقتصد يقرأ الحقول الضرورية فقط

### 🌐 توافق واسع

- **أطر العمل**: `net45` / `net461` / `netstandard2.0` / `netstandard2.1` / `net6.0` / `net8.0` / `net10.0`
- **NativeAOT**: يدعم `net8.0+` التشذيب وNativeAOT (خاصية `IsAotCompatible`، مع اختبار دخان `AotSmoke` ومصفوفة RID رئيسية في CI لحماية الانحدار؛ والمطلوب win-x64 / linux-x64 / linux-arm64؛ ويُغطّى LoongArch linux-la64 عبر محاكاة QEMU (فترة رصد: يعمل، ولا تُعطّل الأعطال البناء))، راجع [تقييم توافق AOT](Doc/AOT兼容性评估.md)
- **MySQL**: 5.5 ~ 9.0+، `mysql_native_password` + `caching_sha2_password`
- **قواعد بيانات متوافقة**: OceanBase وTiDB وDoris (كشف تلقائي)، وAurora / CloudSQL (قواعد سحابية)، وMariaDB (دعم أساسي)
- **الترجمة الشرطية**: اختيار التطبيق الأمثل تلقائيًا لكل إطار عمل

---

## مقارنة الميزات

| الميزة | NewLife.MySql | MySqlConnector | MySql.Data (Oracle) |
|------|:---:|:---:|:---:|
| **الترخيص** | **MIT** | MIT | GPL (مدفوع للاستخدام التجاري) |
| **الاستقلالية المحلية (الصينية)** | ✅ | ❌ | ❌ |
| **تبعيات خارجية** | **لا شيء** | لا شيء | لا شيء |
| **إدخال/إخراج غير متزامن حقيقي** | ✅ | ✅ | ❌ |
| **تنفيذ دفعي مُتدفّق** | ✅ | ❌ | ❌ |
| **دفعات بربط المصفوفات** | ✅ | ❌ | ❌ |
| **دفعات بمجموعات معاملات قاموسية** | ✅ | ❌ | ❌ |
| **DbBatch (.NET 6+)** | ✅ | ✅ | ❌ |
| **DataAdapter** | ✅ | ❌ | ✅ |
| **EF Core** | ✅ | ✅ (Pomelo) | ✅ |
| **دمج ORM XCode** | ✅ | ❌ | ❌ |
| **أطر الهدف** | net45~net10 | netstandard2.0+ | net462+ |
| **NativeAOT (net8.0+)** | ✅ | ✅ | ❌ |
| **تتبّع OpenTelemetry** | ✅ | ✅ | ✅ (8.1+) |
| **OceanBase / TiDB / Aurora / Doris** | ✅ كشف تلقائي | ❌ | ❌ |
| **MySqlBulkCopy** | ✅ | ✅ | ✅ |
| **بروتوكول الضغط (zlib)** | ✅ | ✅ | ❌ |
| **Unix Socket** | ✅ | ✅ | ❌ |
| **مصادقة WebAuthn** | ✅ | ✅ | ❌ |
| **DbDataSource** | ✅ | ❌ | ❌ |

> للتحليل الكامل للميزات والبنية، راجع [البنية](Doc/架构设计.md).

---

## التثبيت

```shell
dotnet add package NewLife.MySql
```

```xml
<PackageReference Include="NewLife.MySql" Version="1.0.*" />
```

---

## البدء السريع

### سلسلة الاتصال

```
Server=localhost;Port=3306;Database=mydb;User Id=root;******;
```

| المعامل | الأسماء البديلة | الافتراضي | الوصف |
|------|------|--------|------|
| Server | DataSource, Data Source | - | عنوان الخادم |
| Port | - | 3306 | رقم المنفذ |
| Database | - | - | اسم قاعدة البيانات |
| UserID | Uid, User Id, User | - | اسم المستخدم |
| Password | Pass, Pwd | - | كلمة المرور |
| ConnectionTimeout | Connection Timeout | 15 | مهلة الاتصال (ثوانٍ) |
| CommandTimeout | Default Command Timeout | 30 | مهلة الأمر (ثوانٍ) |
| SslMode | Ssl Mode | None | وضع SSL (None/Preferred/Required) |
| UseServerPrepare | Use Server Prepare | false | مفتاح التحضير من جانب الخادم على مستوى الاتصال |
| Pipeline | Pipelining | false | مفتاح التنفيذ المُتدفّق |

### فتح اتصال

```csharp
using var conn = new MySqlConnection("Server=localhost;Database=mydb;User Id=root;******;");
conn.Open();
// The connection is closed automatically and returned to the pool when the using scope ends
```

### الاستعلام عن مجموعة نتائج

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

### الاستعلام عن قيمة مفردة

```csharp
using var cmd = new MySqlCommand(conn, "SELECT COUNT(*) FROM users");
var count = cmd.ExecuteScalar();
```

### تنفيذ DML

```csharp
using var cmd = new MySqlCommand(conn, "INSERT INTO users(name, age) VALUES('Tom', 25)");
var affectedRows = cmd.ExecuteNonQuery();

// Shortcut
var rows = conn.ExecuteNonQuery("DELETE FROM logs WHERE created < '2024-01-01'");
```

---

## الاستعلامات ذات المعاملات

استخدم الصيغة `@parameter-name` لربط المعاملات؛ وتُهرَّب القيم تلقائيًا لمنع الحقن:

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

**أنواع المعاملات المدعومة**:

| نوع .NET | مثال على القيمة الحرفية في SQL |
|-----------|---------------|
| `String` | `'hello'` (تُهرَّب الأحرف الخاصة تلقائيًا) |
| `Int32` / `Int64` وأنواع رقمية أخرى | `42` |
| `Boolean` | `1` أو `0` |
| `DateTime` | `'2025-07-01 12:30:00'` |
| `Byte[]` | `X'CAFE'` |
| `MySqlGeometry` | بيانات هندسية بصيغة WKB |
| `Guid` | `'01234567-89ab-cdef-...'` |
| `Enum` | يُحوَّل إلى رقم |
| `null` / `DBNull.Value` | `NULL` |

---

## المعاملات

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

تُدعم أربعة مستويات عزل: `ReadUncommitted` / `ReadCommitted` / `RepeatableRead` (الافتراضي) / `Serializable`

```csharp
using var tr = conn.BeginTransaction(IsolationLevel.ReadCommitted);
```

إذا تم التخلص من معاملة دون تنفيذ Commit أو Rollback، فإنها تُلغى تلقائيًا.

---

## الإجراءات المخزّنة

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

## العبارات المُعدّة مسبقًا

استخدم `Prepare()` لتجميع SQL على الخادم؛ وتستخدم عمليات التنفيذ اللاحقة البروتوكول الثنائي:

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

يمكنك أيضًا تفعيله على مستوى الاتصال عبر سلسلة الاتصال، بحيث تستخدم كل استعلامات المعاملات البروتوكول الثنائي تلقائيًا:

```
UseServerPrepare=true;
```

---

## العمليات الدفعية

### الخيار A: مجموعات معاملات قاموسية

نفّذ نفس SQL مع عدة مجموعات معاملات مربوطة؛ وتُحضَّر داخليًا وتلقائيًا:

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

### الخيار B: ربط المصفوفات (بأسلوب Oracle)

اضبط قيم المعاملات كمصفوفات وحدّد عدد مرات التنفيذ:

```csharp
using var cmd = new MySqlCommand(conn, "INSERT INTO users(name, age) VALUES(@name, @age)");
cmd.Parameters.AddWithValue("name", new[] { "Alice", "Bob", "Charlie" });
cmd.Parameters.AddWithValue("age", new[] { 25, 30, 22 });

var totalAffected = cmd.ExecuteArrayBatch(3);
// or await cmd.ExecuteArrayBatchAsync(3);
```

### الخيار C: التنفيذ المُتدفّق

مكّن `Pipeline=true` في سلسلة الاتصال للإرسال الدفعي والاستقبال الدفعي برحلة شبكة واحدة فقط:

```csharp
var connStr = "Server=localhost;Database=mydb;User Id=root;******;Pipeline=true;";
using var conn = new MySqlConnection(connStr);
conn.Open();

using var cmd = new MySqlCommand(conn, "UPDATE users SET age=@age WHERE name=@name");
cmd.Parameters.AddWithValue("age", agesArray);    // Int32[10000]
cmd.Parameters.AddWithValue("name", namesArray);   // String[10000]
var totalAffected = cmd.ExecuteArrayBatch(10000);
```

يعمل التنفيذ المُتدفّق مع INSERT / UPDATE / DELETE ويمكن دمجه مع مجموعات المعاملات القاموسية أو ربط المصفوفات. وكلما زادت زمن انتقال الشبكة (مثلًا عبر مراكز البيانات)، زادت الفائدة.

### الخيار D: VALUES متعدد الصفوف

```csharp
var sql = "INSERT INTO users(name, age) VALUES('Alice', 25), ('Bob', 30), ('Charlie', 22)";
conn.ExecuteNonQuery(sql);
```

### الخيار E: واجهة DbBatch ‏(.NET 6+)

```csharp
var batch = conn.CreateBatch();
batch.BatchCommands.Add(new MySqlBatchCommand("INSERT INTO users(name, age) VALUES('X1', 10)"));
batch.BatchCommands.Add(new MySqlBatchCommand("INSERT INTO users(name, age) VALUES('X2', 20)"));
batch.BatchCommands.Add(new MySqlBatchCommand("UPDATE users SET age = age + 1 WHERE name = 'X1'"));

using var reader = batch.ExecuteReader();
```

---

## دعم EF Core

توفّر حزمة `NewLife.MySql.EntityFrameworkCore` موفّر قاعدة بيانات لـEF Core يدعم .NET 6 / .NET 8 / .NET 10.

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

**الميزات المدعومة**:

| الميزة | الوصف |
|------|------|
| تعيين الأنواع | Int / Long / String / DateTime / Decimal / Guid / Boolean / Byte[] |
| ترجمة الاستعلامات | LINQ → صيغة MySQL (العلامات الخلفية، LIMIT/OFFSET) |
| ترجمة الدوال | Contains / StartsWith / EndsWith / ToUpper / ToLower / Trim / Replace / Substring / Length |
| ترجمة الأعضاء | DateTime.Year / Month / Day / Hour / Minute / Second |
| دعم ترحيل المخطط | CreateTable / AlterColumn / AddColumn / DropColumn / RenameTable / CreateIndex / DropIndex |
| استراتيجية توليد القيم | مفاتيح أساسية هوية AUTO_INCREMENT |

---

## اتصالات مشفّرة SSL/TLS

```csharp
// Encrypt if the server supports it, otherwise use plaintext
var connStr = "Server=localhost;Database=mydb;User Id=root;******;SslMode=Preferred;";

// Require encryption; throw if the server does not support it
var connStr2 = "Server=localhost;Database=mydb;User Id=root;******;SslMode=Required;";
```

| SslMode | السلوك |
|---------|------|
| `None` / `Disabled` | لا تستخدم SSL (الافتراضي) |
| `Preferred` | تفضيل التشفير، والرجوع إلى النص الصريح إذا لم يكن مدعومًا |
| `Required` | فرض التشفير |

---

## استعلامات معلومات المخطط

```csharp
var databases = conn.GetSchema("Databases");  // Database list
var tables = conn.GetSchema("Tables");         // Table list
var columns = conn.GetSchema("Columns");       // Column information
var indexes = conn.GetSchema("Indexes");       // Index information
```

---

## الطرق غير المتزامنة

تدعم جميع العمليات الأساسية إصدارات غير متزامنة:

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

## التتبّع (OpenTelemetry)

يتضمّن برنامج التشغيل أداة `ActivitySource` مدمجة (اسم المصدر `NewLife.MySql`)، وتُصدر أربعة أنواع من المقاطع: `Open` (الاتصال)، و`Execute` (الأمر، ويغطي عمر القارئ)، و`Commit` / `Rollback` (المعاملات). وتتبع السمات اصطلاحات OpenTelemetry المستقرة الدلالية لقواعد البيانات (`db.system.name` / `db.query.text` / `server.address` / `error.type` وغيرها). ويوفّر المستهلك OpenTelemetry SDK؛ أما برنامج التشغيل فلا يضيف أي تبعيات:

```csharp
using var tracerProvider = Sdk.CreateTracerProviderBuilder()
    .AddSource("NewLife.MySql")
    .AddOtlpExporter()
    .Build();
```

- **النطاق**: `net6.0` وما فوق؛ وفي الإصدارات الأدنى ودون مشتركين يكون بلا عملية وبصفر كلفة
- **ملاحظة الخصوصية**: يسجّل `db.query.text` نص الأمر؛ لذا فضّل الاستعلامات ذات المعاملات كي لا تدخل القيم الحرفية إلى بيانات التتبّع

---

## تجمّع الاتصالات

تجمّع الاتصالات مُفعّل افتراضيًا دون أي إعداد إضافي. وكل سلسلة اتصال فريدة تُقابل تجمّعًا مستقلًا خاصًا بها.

- تُؤخذ الاتصالات من التجمّع تلقائيًا عند الفتح
- تُعاد الاتصالات تلقائيًا عند الإغلاق (ولا يُقطع اتصال TCP)
- تُزال الاتصالات غير الصالحة تلقائيًا
- تُنشأ اتصالات جديدة عند الطلب
- **إعادة الاتصال التلقائي بعد انقطاع الشبكة**: إعادة محاولة تلقائية عند اكتشاف اتصال منقطع، ما يحسّن الاستقرار في السيناريوهات طويلة الأمد

### معاملات تجمّع الاتصالات

| المعامل | الافتراضي | الوصف |
|------|--------|------|
| MinPoolSize | 0 | الحد الأدنى لعدد الاتصالات |
| MaxPoolSize | 100 | الحد الأقصى لعدد الاتصالات |
| ConnectionLifeTime | 0 | عمر الاتصال (ثوانٍ)؛ و0 يعني عدم الانتهاء أبدًا |
| ConnectionIdleTime | 300 | مهلة الخمول (ثوانٍ)؛ وتُستعاد الاتصالات بعد انقضائها |
| ValidationInterval | 30 | فاصل التحقق من الخمول (ثوانٍ) |

```csharp
var connStr = "Server=localhost;Database=mydb;...;MinPoolSize=5;MaxPoolSize=50;ConnectionLifeTime=3600;";
```

---

## دمج ORM XCode

يُسجَّل تلقائيًا كبرنامج تشغيل MySQL لـXCode عبر `MySqlClientFactory`:

```csharp
using XCode.DataAccessLayer;

DAL.AddConnStr("mysql", "Server=localhost;Database=mydb;User Id=root;******;", null, "MySql");

var dal = DAL.Create("mysql");
var tables = dal.Tables;
```

---

## تبديل قواعد البيانات

### ChangeDatabase (موصى به)

```csharp
using var conn = new MySqlConnection("Server=localhost;Database=db1;...");
conn.Open();
conn.ChangeDatabase("db2"); // Internally Close + Reopen
```

> ⚠️ التبديل في منتصف معاملة غير مدعوم (تُفقد المعاملة)؛ فأنشئ اتصالًا مستقلًا لكل قاعدة بيانات.

### SqlClient.SetDatabaseAsync (واجهة منخفضة المستوى)

```csharp
await conn.Client.SetDatabaseAsync("information_schema"); // COM_INIT_DB
```

لا يُغلق الاتصال؛ بل يبدّل قاعدة البيانات الحالية من جانب الخادم فقط، وهو مناسب لسيناريوهات الاستعلام المؤقت.

---

## دعم OceanBase / TiDB

يكتشف برنامج التشغيل نوع قاعدة البيانات تلقائيًا من سلسلة الإصدار في حزمة المصافحة؛ وطريقة الاتصال مطابقة تمامًا لـMySQL:

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

جميعها متوافقة مع SQL الأساسي والمعاملات والاستعلامات ذات المعاملات والعمليات الدفعية.

---

## أطر العمل الهدف

| إطار العمل | الوصف |
|------|------|
| `net45` / `net461` | .NET Framework، متوافق مع المشاريع القديمة |
| `netstandard2.0` / `netstandard2.1` | توافق عبر المنصات، ويدعم Dispose غير المتزامن |
| `net6.0` | يدعم إضافةً واجهة DbBatch |
| `net10.0` | أحدث .NET، وأداء أمثل |

---

## النشر بـNativeAOT

مع أطر العمل الهدف `net8.0+`، اجتاز برنامج التشغيل تحقّقًا حقيقيًا من نشر NativeAOT (قياسات محلية وCI على win-x64 وlinux-x64؛ ونجحت جميع سيناريوهات الدخان الـ26 دون أي تحذير من ILC). ولا يتطلب نشر الثنائيات الأصلية **أي تعديلات من جانب المستدعي**؛ ويحرس CI باستمرار ضد الانحدار عبر مصفوفة RID رئيسية (win-x64 / linux-x64 / linux-arm64 / win-arm64 / osx-x64 / osx-arm64 / musl، إضافةً إلى اختبار دخان محاكى بـQEMU لـLoongArch linux-la64 خلال فترة الرصد، وهو غير معطّل):

```xml
<PropertyGroup>
  <PublishAot>true</PublishAot>
</PropertyGroup>
```

```powershell
dotnet publish -c Release -r linux-x64
```

| البند | المحتوى |
|------|------|
| النطاق | `net8.0+`؛ ويتطلب `net6.0` مفتاحًا تجريبيًا، ولا تُدعم الأطر الأقدم |
| القدرات المتحقَّقة | الاتصال/المصادقة (بما فيها سمات اتصال المصافحة)، والاستعلامات المتزامنة وغير المتزامنة، والتحضير من جانب الخادم، والدفعات (قاموس/مصفوفة/مصفوفة قيم/مُتدفّق)، والمعاملات، والإجراءات المخزّنة، وبيانات المخطط، وDbDataSource، وDataAdapter، وMySqlBulkCopy، وبروتوكول الضغط، والاشتراك في binlog، والتتبّع |
| ملاحظات | تحمل عائلة `MySqlDataAdapter` الخاصة بـFill/FillSchema/Update سمات `RequiresUnreferencedCode`؛ وتعلن التطبيقات المُشذّبة مسؤولية الحفظ في موقع الاستدعاء وفق دلالات System.Data |
| التقييم التفصيلي | [تقييم توافق AOT](Doc/AOT兼容性评估.md) |

---

## ملاحظات

1. **إصدار MySQL**: الإصدار 5.x وما فوق مدعوم، ويُفضّل 8.0+
2. **ترميز الأحرف**: UTF-8 افتراضيًا
3. **ChangeDatabase**: يُنفّذ عبر Close/Reopen؛ والتبديل في منتصف معاملة غير مدعوم، لذا استخدم اتصالًا مستقلًا
4. **التنفيذ المُتدفّق**: لعمليات DML الدفعية فقط، وليس لاستعلامات SELECT

---

## وثائق ذات صلة

| المستند | الوصف |
|------|------|
| [المتطلبات](Doc/需求文档.md) | الرؤية والأهداف الأساسية واتجاهات الميزات |
| [قائمة الميزات](Doc/功能清单.md) | تتبّع ثلاثي الأبعاد للتنفيذ/الاختبار/التعليقات |
| [البنية](Doc/架构设计.md) | نظرة عامة على البنية، وتنفيذ البروتوكول، والخيارات التقنية، وقرارات التصميم، ومقارنة برامج التشغيل |
| [تقرير اختبار الأداء](Doc/性能测试报告.md) | بيانات قياس تفصيلية للعمليات الدفعية |
| [سجل التغييرات (صيني)](Doc/ChangeLog.md) | سجل إصدارات صيني |
| [سجل التغييرات (إنجليزي)](Doc/ChangeLog.en.md) | سجل إصدارات إنجليزي |
| [README بالصينية المبسطة](README.zh-CN.md) | التوثيق الصيني، ويُصان في نفس الدفعة مع هذه النسخة الإنجليزية |
| [دليل الترحيل: MySql.Data → NL](Doc/迁移指南_MySqlData.md) | خطوات وملاحظات الترحيل من برنامج التشغيل الرسمي لـOracle |
| [دليل الترحيل: MySqlConnector → NL](Doc/迁移指南_MySqlConnector.md) | خطوات وملاحظات الترحيل من برنامج التشغيل المجتمعي الشائع |

## خريطة المنظومة

NewLife منظومة متعدّدة الطبقات من مشاريع مفتوحة المصدر تُصدَر بإصدارات مستقلة. الخريطة أدناه مشتركة حرفيًا بين جميع مستودعات NewLife؛ وكل مشروع يرتبط بمستودعه وبحزمته الرئيسية على NuGet.

| المشروع | الوظيفة | التنزيلات |
| --- | --- | --- |
| **الأساس** | | |
| [NewLife.Core](https://github.com/NewLifeX/X) | التسجيل والتهيئة والتخزين المؤقت والشبكات والتسلسل والجدولة وتتبّع أداء APM. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Core?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Core) |
| [NewLife.XCode](https://github.com/NewLifeX/NewLife.XCode) | وسيط بيانات: جداول واحدة بمستوى عشرات المليارات من الصفوف، وتقسيم تلقائي للجداول، وفصل القراءة عن الكتابة، وأكثر من 10 قواعد بيانات. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.XCode?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.XCode) |
| [NewLife.Net](https://github.com/NewLifeX/NewLife.Net) | مكتبة شبكات عالية الأداء: 1.77 مليون pps / 14.5 Gbps على جهاز واحد (TCP 1KB، 2026-10)، و4 ملايين اتصال TCP طويل الأمد. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Net?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Net) |
| [NewLife.Remoting](https://github.com/NewLifeX/NewLife.Remoting) | إطار اتصال بروتوكولي وRPC لتطبيقات العميل/الخادم وأجهزة إنترنت الأشياء. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Remoting?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Remoting) |
| [NewLife.Agent](https://github.com/NewLifeX/NewLife.Agent) | يثبّت تطبيقات .NET كخدمات حارسة لنظام التشغيل: خدمات Windows وsystemd في Linux. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Agent?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Agent) |
| [NewLife.Templates](https://github.com/NewLifeX/NewLife.Zero) | قوالب مشاريع `dotnet new` للطرفية وweb وAPI والخدمات والبيانات وإنترنت الأشياء. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Templates?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Templates) |
| **البيانات والمراسلة** | | |
| [NewLife.Redis](https://github.com/NewLifeX/NewLife.Redis) | عميل Redis بزمن استجابة بالميكروثانية، وملايين العمليات في الثانية، وقوائم رسائل مدمجة. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Redis?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Redis) |
| [NewLife.RocketMQ](https://github.com/NewLifeX/NewLife.RocketMQ) | عميل RocketMQ مُدار بالكامل لـ Apache RocketMQ وخدمة رسائل Alibaba Cloud. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.RocketMQ?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.RocketMQ) |
| [NewLife.MQTT](https://github.com/NewLifeX/NewLife.MQTT) | عميل وخادم MQTT، بما في ذلك الاتصال بـ Alibaba Cloud IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.MQTT?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.MQTT) |
| [NewLife.NovaDb](https://github.com/NewLifeX/NewLife.NovaDb) | قاعدة بيانات هجينة بلغة C#: مدمجة أو شبكية، وعلائقية، وسلاسل زمنية، وقائمة رسائل، وKV في واحد. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.NovaDb?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.NovaDb) |
| [NewLife.MySql](https://github.com/NewLifeX/NewLife.MySql) | برنامج تشغيل MySql وطبقة مساعدة مبنية على مكدّس بيانات NewLife. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.MySql?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.MySql) |
| **Web والذكاء الاصطناعي والأنظمة الموزّعة** | | |
| [NewLife.Cube](https://github.com/NewLifeX/NewLife.Cube) | إطار منصة إدارة سريعة: الصلاحيات، والدخول الموحّد SSO، وخادم OAuth، وتوليد واجهات CRUD. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Cube?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Cube) |
| [Stardust](https://github.com/NewLifeX/Stardust) | منصة خدمات موزّعة: إدارة العقد، وAPM، ومركز الإعدادات، ومركز التسجيل، ومركز النشر. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Stardust?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Stardust) |
| [AntJob](https://github.com/NewLifeX/AntJob) | منصة موزّعة للمهام وجدولة البيانات الفورية، مع التقسيم وإعادة المحاولة وإعادة التعبئة. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.AntJob?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.AntJob) |
| [NewLife.AI](https://github.com/NewLifeX/NewLife.AI) | عميل موحّد لـ 46 مزوّد LLM بالإضافة إلى ChatAI، تطبيق ويب جاهز للمحادثة. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.AI?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.AI) |
| **الصناعة وإنترنت الأشياء** | | |
| [NewLife.IoT](https://github.com/NewLifeX/NewLife.IoT) | مكتبة معايير إنترنت الأشياء التي تعرّف عقود البروتوكولات وتجريدات الأجهزة. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.IoT?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.IoT) |
| [NewLife.Modbus](https://github.com/NewLifeX/NewLife.Modbus) | تنفيذ Modbus TCP/RTU/ASCII فوق NewLife.IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Modbus?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Modbus) |
| [NewLife.Siemens](https://github.com/NewLifeX/NewLife.Siemens) | بروتوكول Siemens PLC فوق NewLife.IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Siemens?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Siemens) |
| [NewLife.Omron](https://github.com/NewLifeX/NewLife.Omron) | بروتوكول Omron PLC فوق NewLife.IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Omron?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Omron) |
| [NewLife.Melsec](https://github.com/NewLifeX/NewLife.Melsec) | بروتوكول Mitsubishi Melsec PLC فوق NewLife.IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Melsec?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Melsec) |
| [NewLife.OPC](https://github.com/NewLifeX/NewLife.OPC) | عميل OPC للوصول إلى البيانات الصناعية. | — |
| [NewLife.BACnet](https://github.com/NewLifeX/NewLife.BACnet) | مكدّس بروتوكول BACnet لأتمتة المباني. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.BACnet?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.BACnet) |
| [ZeroIoT](https://github.com/NewLifeX/ZeroIoT) | منصة إنترنت أشياء مفتوحة المصدر وخفيفة، مع إدارة البوابات والأجهزة. | — |
| **تليماتيكا المركبات** | | |
| [NewLife.JT808](https://github.com/NewLifeX/NewLife.JT808) | بروتوكول تحديد مواقع المركبات JT/T 808: أكثر من 70 جسم رسالة، 2011/2013/2019. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT808?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT808) |
| [NewLife.JT809](https://github.com/NewLifeX/NewLife.JT809) | بروتوكول JT/T 809 بين المنصات لجهات إدارة النقل. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT809?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT809) |
| [NewLife.JT1078](https://github.com/NewLifeX/NewLife.JT1078) | فيديو داخل المركبة JT/T 1078: تحليل RTP وإعادة التجميع إلى FLV/fMP4 والبث المباشر. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT1078?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT1078) |
| **مكوّنات المجال** | | |
| [NewLife.Office](https://github.com/NewLifeX/NewLife.Office) | حزمة واحدة لـ Excel وWord وPPT وPDF وأكثر من 15 صيغة مستندات. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Office?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Office) |
| [NewLife.Audio](https://github.com/NewLifeX/NewLife.Audio) | صوت عبر المنصات: مرمّزات، وإدخال/إخراج الأجهزة، وDSP، وحاويات، وبث. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Audio?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Audio) |
| [NewLife.Video](https://github.com/NewLifeX/NewLife.Video) | مرمّزات الفيديو وصيغ الحاويات ومعالجة الإطارات والبث. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Video?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Video) |
| [NewLife.Map](https://github.com/NewLifeX/NewLife.Map) | واجهة موحّدة فوق Baidu وAMap وTencent وTianditu مع الترميز الجغرافي وتحويل الإحداثيات. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Map?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Map) |
| [NewLife.IP](https://github.com/NewLifeX/NewLife.IP) | تحديد الموقع الجغرافي لعناوين IPv4 بقاعدة بيانات غير متصلة مُعيّنة في الذاكرة، وواجهة API عبر الإنترنت كبديل. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.IP?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.IP) |
| **الأدوات والمنتجات** | | |
| [CrazyCoder](https://github.com/NewLifeX/XCoder) | صندوق أدوات المطوّر: الشبكات والمنفذ التسلسلي والتشفير والتعبيرات النمطية وModbus وMQTT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.XCoder?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.XCoder) |
| [GitCandy](https://github.com/NewLifeX/GitCandy) | نظام استضافة ذاتية لإدارة الشيفرة المصدرية Git. | — |
| [LuckyClover](https://github.com/NewLifeX/LuckyClover) | يثبّت بيئة تشغيل .NET المطابقة تلقائيًا عند التشغيل من مضيف أقدم. | — |

> تعرض الشارات الحزمة الرئيسية لكل مشروع؛ أما حزم السمات والإضافات والحزم التابعة فمذكورة في صفحاتها على NuGet. والحزم التي تتجاوز 30 حزمة وغير المذكورة هنا (FTP وLoRa وإضافات Office وبرامج تشغيل البروتوكولات وغيرها) متاحة في [منظمة NewLifeX](https://github.com/NewLifeX) وعلى [newlifex.com](https://newlifex.com).

## الفريق

<img src="https://newlifex.com/logo.png" width="48" align="right" alt="NewLife" />

تأسّست NewLife في عام 2002، وأمضت أكثر من عقدين في منظومة .NET وإنترنت الأشياء، وتدير ما يقارب 100 مشروع مفتوح المصدر (34 منها مشاريع رئيسية في هذه الخريطة)، وتقدّم حلولًا متكاملة للبرمجيات والأجهزة لقطاعات الكهرباء والخدمات اللوجستية والرعاية الصحية والتحكّم الصناعي والإنترنت. ونلتزم بمبدأ "لا أسبقية في التعلّم — الإتقان يكسب الاحترام".

نُزّلت المكوّنات مفتوحة المصدر <!--nuget-total:start-->6.48M<!--nuget-total:end--> مرة من NuGet (على مستوى المنظومة بأكملها، حتى 2026-10).

الموقع: https://newlifex.com · المنظمة مفتوحة المصدر: https://github.com/NewLifeX

<details>
<summary>حساب WeChat الرسمي: 智能大石头 (امسح للاشتراك)</summary>

![智能大石头](https://newlifex.com/stone.jpg)

</details>
