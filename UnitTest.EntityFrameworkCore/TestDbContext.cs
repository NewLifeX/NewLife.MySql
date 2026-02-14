using Microsoft.EntityFrameworkCore;
using NewLife.MySql.EntityFrameworkCore;

namespace UnitTest.EntityFrameworkCore;

/// <summary>测试用实体</summary>
public class TestUser
{
    public Int32 Id { get; set; }
    public String Name { get; set; } = null!;
    public Int32 Age { get; set; }
    public DateTime CreatedAt { get; set; }
}

/// <summary>测试用 DbContext</summary>
public class TestDbContext : DbContext
{
    public DbSet<TestUser> Users { get; set; } = null!;

    public TestDbContext(DbContextOptions<TestDbContext> options) : base(options) { }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<TestUser>(entity =>
        {
            entity.ToTable("users");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Name).HasMaxLength(100).IsRequired();
            entity.Property(e => e.Age);
            entity.Property(e => e.CreatedAt).HasColumnType("DATETIME");
        });
    }
}
