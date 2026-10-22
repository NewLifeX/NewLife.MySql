# NewLife.MySql — Pilote MySQL hautes performances en C# pur

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
🌐 [English](README.md) | [简体中文](README.zh-CN.md) | [繁體中文](README.zh-TW.md) | [Español](README.es.md) | [Deutsch](README.de.md) | [日本語](README.ja.md) | [Português](README.pt.md) | [Русский](README.ru.md) | [한국어](README.ko.md) | [العربية](README.ar.md)
<!-- lang:end -->

**NewLife.MySql** est un pilote client MySQL en C# pur développé par l'équipe NewLife. Construit sur les interfaces standard ADO.NET, il implémente directement la couche protocole MySQL (Protocol Version 10) au-dessus de TCP. Il ne dépend que de `NewLife.Core`, avec **zéro dépendance tierce**, une **asynchronie réelle de bout en bout** et une **licence MIT** pour un usage commercial sans arrière-pensée.

Son **exécution par lots en pipeline** (Pipeline) innovante apporte une accélération de **2× à 3×** sur les écritures par lots de l'ordre de dizaines de milliers de lignes (par rapport à la référence ligne par ligne), ce qui en fait le choix idéal pour les opérations par lots sur de grands volumes de données et pour les scénarios de substitution en environnement 信创 (souveraineté technologique).

---

## Objectifs de conception

| Objectif | Description |
|------|------|
| **Léger et minimal** | Uniquement les opérations ADO.NET standard : CRUD, transactions, requêtes paramétrées, procédures stockées et opérations par lots |
| **Hautes performances** | Zéro allocation mémoire supplémentaire (`ArrayPool` / `OwnerPacket`), E/S asynchrones réelles, exécution par lots en pipeline |
| **Pile 100 % nationale (chinoise)** | Code cœur entièrement maîtrisé, sans risque de licence GPL, adapté aux environnements 信创 |
| **Adapté aux gros volumes** | Ensembles de paramètres en dictionnaire / liaison de tableaux / pipeline / VALUES multi-lignes / DbBatch, des dizaines de milliers aux millions de lignes |
| **Large compatibilité** | `net45` ~ `net10` pour toutes les versions, MySQL 5.x ~ 9.0+, compatible OceanBase / TiDB / Doris / Aurora / CloudSQL, et prise en charge de NativeAOT à partir de `net8.0+` |

## Fonctionnalités principales

| Fonctionnalité | Description |
|------|------|
| Gestion des connexions | MySQL 5.x / 8.0 / 9.0+, plusieurs méthodes d'authentification : `mysql_native_password` + `caching_sha2_password` + `authentication_webauthn` |
| Unix Socket | Prise en charge des connexions locales par socket de domaine Unix, sans surcoût TCP |
| Protocole de compression | Transport compressé zlib (`UseCompression`) pour réduire la consommation de bande passante ; zstd est différé en raison de la contrainte de zéro dépendance tierce |
| Exécution de requêtes | `ExecuteReader` / `ExecuteScalar` / `ExecuteNonQuery`, interfaces ADO.NET complètes |
| Requêtes paramétrées | Substitution de paramètres côté client avec la syntaxe `@nom-du-parametre` et échappement automatique contre l'injection |
| Instructions préparées | `Prepare()` utilise le protocole binaire `COM_STMT_PREPARE`, avec un commutateur global optionnel `UseServerPrepare` |
| Opérations par lots | `ExecuteBatch` / `ExecuteArrayBatch` / pipeline / VALUES multi-lignes / DbBatch / `MySqlBulkCopy` |
| Exécution en pipeline | `Pipeline=true` envoie et reçoit par lots, avec un seul aller-retour réseau : 35× pour des milliers de lignes et près de 100× pour des dizaines de milliers de lignes |
| Transactions | `BeginTransaction` / `Commit` / `Rollback`, quatre niveaux d'isolation, annulation automatique |
| Procédures stockées | `CommandType.StoredProcedure`, prise en charge des paramètres IN/OUT |
| SSL/TLS | `SslMode=Preferred/Required`, prise en charge de TLS 1.2/1.3 |
| Pool de connexions | Activé par défaut, regroupé par chaîne de connexion, durée de vie et validation d'inactivité configurables, vérifications de santé et recyclage automatiques |
| Méthodes asynchrones | Chaîne complète `async/await` : `OpenAsync` / `ExecuteReaderAsync` / `ReadAsync`, etc. |
| Requêtes de schéma | Métadonnées Tables / Columns / Indexes / Databases, plus des requêtes étendues sur les index et clés étrangères |
| DbDataSource | `MySqlDataSource` de style .NET 7+, prise en charge de l'injection de dépendances |
| Traçage | Instrumentation `ActivitySource` intégrée (nom de source `NewLife.MySql`), émettant des spans Open / Execute / Commit / Rollback conformes aux conventions sémantiques stables d'OpenTelemetry, sans surcoût en l'absence d'abonné |
| DbBatch | API de lots ADO.NET standard de .NET 6+ |
| Données spatiales | Types Geometry de MySQL, enveloppe WKB `MySqlGeometry` |
| EF Core | Pris en charge via le paquet séparé `NewLife.MySql.EntityFrameworkCore` |
| Intégration XCode | Enregistrement automatique via `MySqlClientFactory`, intégration transparente avec l'ORM XCode |

---

## Points forts

### 🚀 Exécution par lots en pipeline (Pipeline)

Un mode pipeline innovant fondé sur le protocole binaire `COM_STMT_PREPARE + COM_STMT_EXECUTE` :

- **Phase 1** : construire et envoyer tous les paquets EXECUTE par lots, en ne vidant que le dernier paquet
- **Phase 2** : lire toutes les réponses OK/Error dans l'ordre par lots, en cumulant le nombre de lignes affectées
- La pile TCP regroupe les petits paquets (algorithme de Nagle), ce qui ne produit qu'un seul aller-retour réseau

**Données mesurées** (.NET 10 + MySQL 8.0.26 en localhost, pipeline + transaction comparé à la référence ligne par ligne) :

| Scénario | Référence ligne par ligne | Pipeline(tx) | Accélération |
|------|------:|------:|------:|
| INSERT de 100 lignes | 244ms | 20ms | 12,2× |
| INSERT de 1 000 lignes | 2 473ms | 70ms | **35,3×** |
| INSERT de 10 000 lignes | ~70s¹ | ~0,7s¹ | ~98× |

> ¹ Le chiffre ligne par ligne pour 10 000 lignes sans transaction est extrapolé (2,4 ms/ligne) ; le chiffre Pipeline est mesuré sans transaction. Avec une transaction, 10 000 lignes prennent environ 70 ms.

**Performances des trois pilotes** (opérations par lots de 10 000 lignes) :

| Opération | NewLife Pipeline(tx) | MySql.Data Batch(tx) | MySqlConnector Batch(tx) |
|------|------:|------:|------:|
| INSERT | 899ms | 1 927ms | 1 906ms |
| UPDATE | 710ms | 2 265ms | 2 041ms |
| DELETE | 661ms | 1 961ms | 1 767ms |

> Quelques milliers de lignes suffisent déjà à obtenir une accélération de **35×** sur un réseau local ; le gain est encore plus marqué entre centres de données. Voir le [rapport de test de performance](Doc/性能测试报告.md) pour les données détaillées.

### 🔧 Six options d'opérations par lots

| Option | API | Cas d'usage |
|------|-----|---------|
| Ensembles de paramètres en dictionnaire | `ExecuteBatch` | Paramètres dynamiques fournis par une collection |
| Liaison de tableaux de valeurs | `ExecuteBatchValues` | Import en masse (interne à `MySqlBulkCopy`), zéro allocation de dictionnaire ou d'objet paramètre |
| Liaison de tableaux | `ExecuteArrayBatch` | DML à gros volume, style Oracle |
| Pipeline | `Pipeline=true` | Dizaines ou centaines de milliers de lignes, entre centres de données |
| VALUES multi-lignes | SQL concaténé | Lots d'INSERT purs |
| DbBatch (.NET 6+) | `CreateBatch` | Lots mixtes d'instructions SQL différentes |

### 🛡️ Zéro dépendance tierce

Dépend uniquement de `NewLife.Core` (également de l'équipe NewLife), sans risque de licence GPL, sous licence MIT pour un usage commercial sans arrière-pensée, adapté aux environnements 信创 et aux scénarios à audit de sécurité strict.

### ⚡ Optimisations de performance extrêmes

- `ArrayPool<T>` + `OwnerPacket` pour zéro allocation supplémentaire
- `Pool.StringBuilder` / `Pool.MemoryStream` entièrement mutualisés
- Chaîne complète `async/await`, sans anti-patron `sync-over-async`
- Analyse de protocole allégée ne lisant que les champs nécessaires

### 🌐 Large compatibilité

- **Frameworks** : `net45` / `net461` / `netstandard2.0` / `netstandard2.1` / `net6.0` / `net8.0` / `net10.0`
- **NativeAOT** : `net8.0+` prend en charge le trimming et NativeAOT (`IsAotCompatible`, avec un test de fumée `AotSmoke` et une matrice CI des RID courants pour la protection contre les régressions ; win-x64 / linux-x64 / linux-arm64 requis ; LoongArch linux-la64 est couvert par émulation QEMU (période d'observation : il s'exécute, les échecs ne bloquent pas)), voir [évaluation de compatibilité AOT](Doc/AOT兼容性评估.md)
- **MySQL** : 5.5 ~ 9.0+, `mysql_native_password` + `caching_sha2_password`
- **Bases compatibles** : OceanBase, TiDB, Doris (détection automatique), Aurora / CloudSQL (bases cloud), MariaDB (basique)
- **Compilation conditionnelle** : sélection automatique de l'implémentation optimale par framework

---

## Comparaison des fonctionnalités

| Fonctionnalité | NewLife.MySql | MySqlConnector | MySql.Data (Oracle) |
|------|:---:|:---:|:---:|
| **Licence** | **MIT** | MIT | GPL (payante pour un usage commercial) |
| **Autonomie nationale (chinoise)** | ✅ | ❌ | ❌ |
| **Dépendances tierces** | **Aucune** | Aucune | Aucune |
| **E/S asynchrones réelles** | ✅ | ✅ | ❌ |
| **Exécution par lots en pipeline** | ✅ | ❌ | ❌ |
| **Lots par liaison de tableaux** | ✅ | ❌ | ❌ |
| **Lots par ensembles de paramètres en dictionnaire** | ✅ | ❌ | ❌ |
| **DbBatch (.NET 6+)** | ✅ | ✅ | ❌ |
| **DataAdapter** | ✅ | ❌ | ✅ |
| **EF Core** | ✅ | ✅ (Pomelo) | ✅ |
| **Intégration ORM XCode** | ✅ | ❌ | ❌ |
| **Frameworks cibles** | net45~net10 | netstandard2.0+ | net462+ |
| **NativeAOT (net8.0+)** | ✅ | ✅ | ❌ |
| **Traçage OpenTelemetry** | ✅ | ✅ | ✅ (8.1+) |
| **OceanBase / TiDB / Aurora / Doris** | ✅ Détection automatique | ❌ | ❌ |
| **MySqlBulkCopy** | ✅ | ✅ | ✅ |
| **Protocole de compression (zlib)** | ✅ | ✅ | ❌ |
| **Unix Socket** | ✅ | ✅ | ❌ |
| **Authentification WebAuthn** | ✅ | ✅ | ❌ |
| **DbDataSource** | ✅ | ❌ | ❌ |

> Pour l'analyse complète des fonctionnalités et de l'architecture, voir [l'architecture](Doc/架构设计.md).

---

## Installation

```shell
dotnet add package NewLife.MySql
```

```xml
<PackageReference Include="NewLife.MySql" Version="1.0.*" />
```

---

## Démarrage rapide

### Chaîne de connexion

```
Server=localhost;Port=3306;Database=mydb;User Id=root;******;
```

| Paramètre | Alias | Valeur par défaut | Description |
|------|------|--------|------|
| Server | DataSource, Data Source | - | Adresse du serveur |
| Port | - | 3306 | Numéro de port |
| Database | - | - | Nom de la base de données |
| UserID | Uid, User Id, User | - | Nom d'utilisateur |
| Password | Pass, Pwd | - | Mot de passe |
| ConnectionTimeout | Connection Timeout | 15 | Délai de connexion (secondes) |
| CommandTimeout | Default Command Timeout | 30 | Délai d'exécution des commandes (secondes) |
| SslMode | Ssl Mode | None | Mode SSL (None/Preferred/Required) |
| UseServerPrepare | Use Server Prepare | false | Commutateur global de préparation côté serveur |
| Pipeline | Pipelining | false | Commutateur d'exécution en pipeline |

### Ouvrir une connexion

```csharp
using var conn = new MySqlConnection("Server=localhost;Database=mydb;User Id=root;******;");
conn.Open();
// The connection is closed automatically and returned to the pool when the using scope ends
```

### Interroger un jeu de résultats

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

### Interroger une valeur scalaire

```csharp
using var cmd = new MySqlCommand(conn, "SELECT COUNT(*) FROM users");
var count = cmd.ExecuteScalar();
```

### Exécuter une instruction DML

```csharp
using var cmd = new MySqlCommand(conn, "INSERT INTO users(name, age) VALUES('Tom', 25)");
var affectedRows = cmd.ExecuteNonQuery();

// Shortcut
var rows = conn.ExecuteNonQuery("DELETE FROM logs WHERE created < '2024-01-01'");
```

---

## Requêtes paramétrées

Utilisez la syntaxe `@nom-du-parametre` pour lier les paramètres ; les valeurs sont échappées automatiquement contre l'injection :

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

**Types de paramètres pris en charge** :

| Type .NET | Exemple de littéral SQL |
|-----------|---------------|
| `String` | `'hello'` (caractères spéciaux échappés automatiquement) |
| `Int32` / `Int64` et autres types numériques | `42` |
| `Boolean` | `1` ou `0` |
| `DateTime` | `'2025-07-01 12:30:00'` |
| `Byte[]` | `X'CAFE'` |
| `MySqlGeometry` | Données de géométrie au format WKB |
| `Guid` | `'01234567-89ab-cdef-...'` |
| `Enum` | Converti en nombre |
| `null` / `DBNull.Value` | `NULL` |

---

## Transactions

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

Quatre niveaux d'isolation sont pris en charge : `ReadUncommitted` / `ReadCommitted` / `RepeatableRead` (par défaut) / `Serializable`

```csharp
using var tr = conn.BeginTransaction(IsolationLevel.ReadCommitted);
```

Si une transaction est libérée sans être validée ni annulée, elle est annulée automatiquement.

---

## Procédures stockées

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

## Instructions préparées

Utilisez `Prepare()` pour compiler le SQL sur le serveur ; les exécutions suivantes utilisent le protocole binaire :

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

Vous pouvez aussi l'activer globalement via la chaîne de connexion, afin que chaque requête paramétrée utilise automatiquement le protocole binaire :

```
UseServerPrepare=true;
```

---

## Opérations par lots

### Option A : ensembles de paramètres en dictionnaire

Exécutez le même SQL avec plusieurs jeux de paramètres liés ; la préparation est faite en interne et automatiquement :

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

### Option B : liaison de tableaux (style Oracle)

Définissez les valeurs des paramètres sous forme de tableaux et indiquez le nombre d'exécutions :

```csharp
using var cmd = new MySqlCommand(conn, "INSERT INTO users(name, age) VALUES(@name, @age)");
cmd.Parameters.AddWithValue("name", new[] { "Alice", "Bob", "Charlie" });
cmd.Parameters.AddWithValue("age", new[] { 25, 30, 22 });

var totalAffected = cmd.ExecuteArrayBatch(3);
// or await cmd.ExecuteArrayBatchAsync(3);
```

### Option C : exécution en pipeline

Activez `Pipeline=true` dans la chaîne de connexion pour envoyer et recevoir par lots avec un seul aller-retour réseau :

```csharp
var connStr = "Server=localhost;Database=mydb;User Id=root;******;Pipeline=true;";
using var conn = new MySqlConnection(connStr);
conn.Open();

using var cmd = new MySqlCommand(conn, "UPDATE users SET age=@age WHERE name=@name");
cmd.Parameters.AddWithValue("age", agesArray);    // Int32[10000]
cmd.Parameters.AddWithValue("name", namesArray);   // String[10000]
var totalAffected = cmd.ExecuteArrayBatch(10000);
```

Le pipeline fonctionne pour INSERT / UPDATE / DELETE et peut être combiné avec des ensembles de paramètres en dictionnaire ou la liaison de tableaux. Plus la latence réseau est élevée (par exemple entre centres de données), plus le gain est important.

### Option D : VALUES multi-lignes

```csharp
var sql = "INSERT INTO users(name, age) VALUES('Alice', 25), ('Bob', 30), ('Charlie', 22)";
conn.ExecuteNonQuery(sql);
```

### Option E : API DbBatch (.NET 6+)

```csharp
var batch = conn.CreateBatch();
batch.BatchCommands.Add(new MySqlBatchCommand("INSERT INTO users(name, age) VALUES('X1', 10)"));
batch.BatchCommands.Add(new MySqlBatchCommand("INSERT INTO users(name, age) VALUES('X2', 20)"));
batch.BatchCommands.Add(new MySqlBatchCommand("UPDATE users SET age = age + 1 WHERE name = 'X1'"));

using var reader = batch.ExecuteReader();
```

---

## Prise en charge d'EF Core

Le paquet `NewLife.MySql.EntityFrameworkCore` fournit un fournisseur de base de données EF Core prenant en charge .NET 6 / .NET 8 / .NET 10.

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

**Fonctionnalités prises en charge** :

| Fonctionnalité | Description |
|------|------|
| Mappage de types | Int / Long / String / DateTime / Decimal / Guid / Boolean / Byte[] |
| Traduction des requêtes | LINQ → syntaxe MySQL (backticks, LIMIT/OFFSET) |
| Traduction des méthodes | Contains / StartsWith / EndsWith / ToUpper / ToLower / Trim / Replace / Substring / Length |
| Traduction des membres | DateTime.Year / Month / Day / Hour / Minute / Second |
| Prise en charge des migrations | CreateTable / AlterColumn / AddColumn / DropColumn / RenameTable / CreateIndex / DropIndex |
| Stratégie de génération de valeurs | Clés primaires d'identité AUTO_INCREMENT |

---

## Connexions chiffrées SSL/TLS

```csharp
// Encrypt if the server supports it, otherwise use plaintext
var connStr = "Server=localhost;Database=mydb;User Id=root;******;SslMode=Preferred;";

// Require encryption; throw if the server does not support it
var connStr2 = "Server=localhost;Database=mydb;User Id=root;******;SslMode=Required;";
```

| SslMode | Comportement |
|---------|------|
| `None` / `Disabled` | Ne pas utiliser SSL (par défaut) |
| `Preferred` | Préférer le chiffrement, revenir au texte en clair si non pris en charge |
| `Required` | Forcer le chiffrement |

---

## Requêtes d'informations de schéma

```csharp
var databases = conn.GetSchema("Databases");  // Database list
var tables = conn.GetSchema("Tables");         // Table list
var columns = conn.GetSchema("Columns");       // Column information
var indexes = conn.GetSchema("Indexes");       // Index information
```

---

## Méthodes asynchrones

Toutes les opérations principales disposent de versions asynchrones :

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

## Traçage (OpenTelemetry)

Le pilote intègre une instrumentation `ActivitySource` (nom de source `NewLife.MySql`), émettant quatre types de spans : `Open` (connexion), `Execute` (commande, couvrant la durée de vie du reader), `Commit` / `Rollback` (transactions). Les attributs suivent les conventions sémantiques stables de base de données d'OpenTelemetry (`db.system.name` / `db.query.text` / `server.address` / `error.type`, etc.). Le SDK OpenTelemetry est fourni par le consommateur ; le pilote n'ajoute aucune dépendance :

```csharp
using var tracerProvider = Sdk.CreateTracerProviderBuilder()
    .AddSource("NewLife.MySql")
    .AddOtlpExporter()
    .Build();
```

- **Portée** : `net6.0` et au-delà ; sur les versions inférieures et sans abonné, il s'agit d'une opération neutre sans surcoût
- **Note de confidentialité** : `db.query.text` enregistre le texte de la commande ; privilégiez les requêtes paramétrées pour que les littéraux n'entrent pas dans la télémétrie

---

## Pool de connexions

Le pool de connexions est activé par défaut, sans configuration supplémentaire. Chaque chaîne de connexion unique correspond à son propre pool indépendant.

- Les connexions sont obtenues automatiquement du pool à l'ouverture
- Les connexions sont restituées automatiquement à la fermeture (la connexion TCP n'est pas coupée)
- Les connexions invalides sont supprimées automatiquement
- De nouvelles connexions sont créées à la demande
- **Reconnexion automatique après une coupure réseau** : nouvelle tentative automatique lorsqu'une connexion perdue est détectée, ce qui améliore la stabilité des scénarios de longue durée

### Paramètres du pool de connexions

| Paramètre | Valeur par défaut | Description |
|------|--------|------|
| MinPoolSize | 0 | Nombre minimum de connexions |
| MaxPoolSize | 100 | Nombre maximum de connexions |
| ConnectionLifeTime | 0 | Durée de vie des connexions (secondes) ; 0 signifie jamais d'expiration |
| ConnectionIdleTime | 300 | Délai d'inactivité (secondes) ; les connexions sont récupérées à son expiration |
| ValidationInterval | 30 | Intervalle de validation des connexions inactives (secondes) |

```csharp
var connStr = "Server=localhost;Database=mydb;...;MinPoolSize=5;MaxPoolSize=50;ConnectionLifeTime=3600;";
```

---

## Intégration de l'ORM XCode

Enregistré automatiquement comme pilote MySQL pour XCode via `MySqlClientFactory` :

```csharp
using XCode.DataAccessLayer;

DAL.AddConnStr("mysql", "Server=localhost;Database=mydb;User Id=root;******;", null, "MySql");

var dal = DAL.Create("mysql");
var tables = dal.Tables;
```

---

## Changer de base de données

### ChangeDatabase (recommandé)

```csharp
using var conn = new MySqlConnection("Server=localhost;Database=db1;...");
conn.Open();
conn.ChangeDatabase("db2"); // Internally Close + Reopen
```

> ⚠️ Le changement au milieu d'une transaction n'est pas pris en charge (la transaction est perdue) ; créez une connexion indépendante pour chaque base de données.

### SqlClient.SetDatabaseAsync (API de bas niveau)

```csharp
await conn.Client.SetDatabaseAsync("information_schema"); // COM_INIT_DB
```

Ne ferme pas la connexion ; ne change que la base de données courante côté serveur, adapté aux scénarios de requêtes temporaires.

---

## Prise en charge d'OceanBase / TiDB

Le pilote détecte automatiquement le type de base de données à partir de la chaîne de version du paquet de handshake ; la méthode de connexion est exactement identique à celle de MySQL :

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

Le SQL de base, les transactions, les requêtes paramétrées et les opérations par lots sont tous compatibles.

---

## Frameworks cibles

| Framework | Description |
|------|------|
| `net45` / `net461` | .NET Framework, compatible avec les projets existants |
| `netstandard2.0` / `netstandard2.1` | Compatibilité multiplateforme, prise en charge de Dispose asynchrone |
| `net6.0` | Prise en charge supplémentaire de l'API DbBatch |
| `net10.0` | Dernière version de .NET, performances optimales |

---

## Déploiement NativeAOT

Avec les frameworks cibles `net8.0+`, le pilote a passé une validation de publication NativeAOT réelle (mesures locales et CI sous win-x64 et linux-x64 ; les 26 scénarios de test de fumée sont passés avec 0 avertissement ILC). La publication de binaires natifs ne nécessite **aucun correctif côté appelant** ; la CI protège en continu contre les régressions sur une matrice de RID courants (win-x64 / linux-x64 / linux-arm64 / win-arm64 / osx-x64 / osx-arm64 / musl, plus un test de fumée émulé par QEMU pour LoongArch linux-la64 pendant la période d'observation, non bloquant) :

```xml
<PropertyGroup>
  <PublishAot>true</PublishAot>
</PropertyGroup>
```

```powershell
dotnet publish -c Release -r linux-x64
```

| Élément | Contenu |
|------|------|
| Portée | `net8.0+` ; `net6.0` nécessite un commutateur expérimental, et les frameworks antérieurs ne sont pas pris en charge |
| Capacités vérifiées | Connexion/authentification (y compris les attributs de connexion du handshake), requêtes synchrones et asynchrones, préparation côté serveur, lots (dictionnaire/tableau/tableau de valeurs/pipeline), transactions, procédures stockées, métadonnées de schéma, DbDataSource, DataAdapter, MySqlBulkCopy, protocole de compression, abonnement binlog, traçage |
| Remarques | La famille Fill/FillSchema/Update de `MySqlDataAdapter` porte des annotations `RequiresUnreferencedCode` ; les applications trimmées déclarent la responsabilité de préservation au point d'appel selon la sémantique de System.Data |
| Évaluation détaillée | [Évaluation de compatibilité AOT](Doc/AOT兼容性评估.md) |

---

## Remarques

1. **Version de MySQL** : 5.x et au-delà pris en charge, 8.0+ recommandé
2. **Encodage des caractères** : UTF-8 par défaut
3. **ChangeDatabase** : implémenté via Close/Reopen ; le changement au milieu d'une transaction n'est pas pris en charge, utilisez donc une connexion indépendante
4. **Exécution en pipeline** : uniquement pour les opérations DML par lots, pas pour les requêtes SELECT

---

## Documentation associée

| Document | Description |
|------|------|
| [Exigences](Doc/需求文档.md) | Vision, objectifs principaux, orientations fonctionnelles |
| [Liste des fonctionnalités](Doc/功能清单.md) | Suivi tridimensionnel de l'implémentation/des tests/des commentaires |
| [Architecture](Doc/架构设计.md) | Vue d'ensemble de l'architecture, implémentation du protocole, choix technologiques, décisions de conception, comparaison des pilotes |
| [Rapport de test de performance](Doc/性能测试报告.md) | Données de référence détaillées pour les opérations par lots |
| [Journal des modifications (中文)](Doc/ChangeLog.md) | Journal des versions en chinois |
| [Journal des modifications (EN)](Doc/ChangeLog.en.md) | Journal des versions en anglais |
| [README 简体中文](README.zh-CN.md) | Documentation chinoise, maintenue dans le même lot que cette version anglaise |
| [Guide de migration : MySql.Data → NL](Doc/迁移指南_MySqlData.md) | Étapes et notes pour migrer depuis le pilote officiel d'Oracle |
| [Guide de migration : MySqlConnector → NL](Doc/迁移指南_MySqlConnector.md) | Étapes et notes pour migrer depuis le pilote communautaire le plus répandu |

## Carte de l'écosystème

NewLife est un écosystème en couches de projets open source versionnés indépendamment. La carte ci-dessous est reprise à l'identique par tous les dépôts NewLife ; chaque projet renvoie à son dépôt et à son paquet NuGet principal.

| Projet | Rôle | Téléchargements |
| --- | --- | --- |
| **Fondation** | | |
| [NewLife.Core](https://github.com/NewLifeX/X) | Journalisation, configuration, cache, réseau, sérialisation, ordonnancement et traçage APM. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Core?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Core) |
| [NewLife.XCode](https://github.com/NewLifeX/NewLife.XCode) | Intergiciel de données : tables uniques à l'échelle de dizaines de milliards de lignes, partitionnement automatique, séparation lecture/écriture, plus de 10 bases de données. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.XCode?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.XCode) |
| [NewLife.Net](https://github.com/NewLifeX/NewLife.Net) | Bibliothèque réseau hautes performances : 1,77 M pps / 14,5 Gbps sur une seule machine (TCP 1KB, 2026-10), 4 M de connexions TCP persistantes. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Net?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Net) |
| [NewLife.Remoting](https://github.com/NewLifeX/NewLife.Remoting) | Communication protocolaire et framework RPC pour applications client/serveur et appareils IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Remoting?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Remoting) |
| [NewLife.Agent](https://github.com/NewLifeX/NewLife.Agent) | Installe des applications .NET comme démons du système d'exploitation : services Windows et systemd Linux. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Agent?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Agent) |
| [NewLife.Templates](https://github.com/NewLifeX/NewLife.Zero) | Modèles de projet `dotnet new` pour la console, le web, l'API, les services, les données et l'IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Templates?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Templates) |
| **Données et messagerie** | | |
| [NewLife.Redis](https://github.com/NewLifeX/NewLife.Redis) | Client Redis avec une latence en microsecondes, des millions d'opérations/s et des files de messages intégrées. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Redis?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Redis) |
| [NewLife.RocketMQ](https://github.com/NewLifeX/NewLife.RocketMQ) | Client RocketMQ entièrement managé pour Apache RocketMQ et Alibaba Cloud Message Queue. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.RocketMQ?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.RocketMQ) |
| [NewLife.MQTT](https://github.com/NewLifeX/NewLife.MQTT) | Client et serveur MQTT, y compris la connectivité Alibaba Cloud IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.MQTT?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.MQTT) |
| [NewLife.NovaDb](https://github.com/NewLifeX/NewLife.NovaDb) | Base de données hybride en C# : embarquée ou en réseau, relationnelle, séries temporelles, file de messages et KV réunis. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.NovaDb?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.NovaDb) |
| [NewLife.MySql](https://github.com/NewLifeX/NewLife.MySql) | Pilote MySql et couche d'assistance construits sur la pile de données NewLife. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.MySql?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.MySql) |
| **Web, IA et distribué** | | |
| [NewLife.Cube](https://github.com/NewLifeX/NewLife.Cube) | Framework rapide de plateforme d'administration : permissions, SSO, serveur OAuth et échafaudage CRUD. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Cube?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Cube) |
| [Stardust](https://github.com/NewLifeX/Stardust) | Plateforme de services distribués : gestion des nœuds, APM, centre de configuration, registre et centre de publication. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Stardust?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Stardust) |
| [AntJob](https://github.com/NewLifeX/AntJob) | Plateforme de tâches distribuées et d'ordonnancement de données en temps réel avec partitionnement, reprises et backfill. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.AntJob?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.AntJob) |
| [NewLife.AI](https://github.com/NewLifeX/NewLife.AI) | Client unifié pour 46 fournisseurs de LLM, plus ChatAI, une application web de chat prête à l'emploi. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.AI?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.AI) |
| **Industriel et IoT** | | |
| [NewLife.IoT](https://github.com/NewLifeX/NewLife.IoT) | Bibliothèque standard IoT définissant les contrats de protocole et les abstractions d'appareils. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.IoT?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.IoT) |
| [NewLife.Modbus](https://github.com/NewLifeX/NewLife.Modbus) | Implémentation Modbus TCP/RTU/ASCII au-dessus de NewLife.IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Modbus?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Modbus) |
| [NewLife.Siemens](https://github.com/NewLifeX/NewLife.Siemens) | Protocole pour automate Siemens au-dessus de NewLife.IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Siemens?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Siemens) |
| [NewLife.Omron](https://github.com/NewLifeX/NewLife.Omron) | Protocole pour automate Omron au-dessus de NewLife.IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Omron?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Omron) |
| [NewLife.Melsec](https://github.com/NewLifeX/NewLife.Melsec) | Protocole pour automate Mitsubishi Melsec au-dessus de NewLife.IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Melsec?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Melsec) |
| [NewLife.OPC](https://github.com/NewLifeX/NewLife.OPC) | Client OPC pour l'accès aux données industrielles. | — |
| [NewLife.BACnet](https://github.com/NewLifeX/NewLife.BACnet) | Pile de protocoles BACnet pour l'automatisation des bâtiments. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.BACnet?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.BACnet) |
| [ZeroIoT](https://github.com/NewLifeX/ZeroIoT) | Plateforme IoT open source légère avec gestion des passerelles et des appareils. | — |
| **Télématique embarquée** | | |
| [NewLife.JT808](https://github.com/NewLifeX/NewLife.JT808) | Protocole de positionnement de véhicules JT/T 808 : plus de 70 corps de message, 2011/2013/2019. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT808?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT808) |
| [NewLife.JT809](https://github.com/NewLifeX/NewLife.JT809) | Protocole plateforme à plateforme JT/T 809 pour les autorités de transport. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT809?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT809) |
| [NewLife.JT1078](https://github.com/NewLifeX/NewLife.JT1078) | Vidéo embarquée JT/T 1078 : analyse RTP, remuxage FLV/fMP4 et diffusion en direct. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT1078?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT1078) |
| **Composants métier** | | |
| [NewLife.Office](https://github.com/NewLifeX/NewLife.Office) | Un seul paquet pour Excel, Word, PPT, PDF et plus de 15 formats de documents. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Office?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Office) |
| [NewLife.Audio](https://github.com/NewLifeX/NewLife.Audio) | Audio multiplateforme : codecs, E/S d'appareils, DSP, conteneurs et streaming. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Audio?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Audio) |
| [NewLife.Video](https://github.com/NewLifeX/NewLife.Video) | Codecs vidéo, formats de conteneurs, traitement des images et streaming. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Video?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Video) |
| [NewLife.Map](https://github.com/NewLifeX/NewLife.Map) | Interface unifiée pour Baidu, AMap, Tencent et Tianditu avec géocodage et conversion de coordonnées. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Map?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Map) |
| [NewLife.IP](https://github.com/NewLifeX/NewLife.IP) | Géolocalisation IPv4 avec une base de données hors ligne mappée en mémoire et une API en ligne en secours. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.IP?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.IP) |
| **Outils et produits** | | |
| [CrazyCoder](https://github.com/NewLifeX/XCoder) | Boîte à outils du développeur : réseau, port série, cryptographie, regex, Modbus et MQTT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.XCoder?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.XCoder) |
| [GitCandy](https://github.com/NewLifeX/GitCandy) | Système de gestion de code source Git auto-hébergé. | — |
| [LuckyClover](https://github.com/NewLifeX/LuckyClover) | Installe automatiquement le runtime .NET correspondant lorsqu'il est lancé depuis un hôte plus ancien. | — |

> Les badges indiquent le paquet principal de chaque projet ; les paquets de thèmes, d'extensions et satellites sont répertoriés sur leurs pages NuGet. Les plus de 30 paquets non listés ici (FTP, LoRa, extensions Office, pilotes de protocoles, etc.) sont disponibles dans l'[organisation NewLifeX](https://github.com/NewLifeX) et sur [newlifex.com](https://newlifex.com).

## Équipe

<img src="https://newlifex.com/logo.png" width="48" align="right" alt="NewLife" />

Fondée en 2002, l'équipe NewLife œuvre depuis plus de deux décennies dans l'écosystème .NET et IoT, maintient près de 100 projets open source (dont 34 sont essentiels à cette carte) et fournit des solutions matérielles et logicielles intégrées aux secteurs de l'énergie, de la logistique, de la santé, du contrôle industriel et d'Internet. Notre devise : « l'apprentissage ne connaît pas l'ancienneté — celui qui maîtrise devient maître ».

Les composants open source ont été téléchargés <!--nuget-total:start-->6.48M<!--nuget-total:end--> fois depuis NuGet (à l'échelle de l'écosystème, en date de 2026-10).

Site web : https://newlifex.com · Organisation open source : https://github.com/NewLifeX

<details>
<summary>Compte officiel WeChat : 智能大石头 (scannez pour suivre)</summary>

![智能大石头](https://newlifex.com/stone.jpg)

</details>
