using Npgsql;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using Z.Dapper.Plus;

namespace dbperfornance.net
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            Dapper();
            Console.ReadLine();
        }

        private static int numberOfEntities = 50_000;

        private static List<TestEntity> PrepareEntities()
        {
            var retList = new List<TestEntity>(numberOfEntities);

            for (int i = 0; i < numberOfEntities; i++)
            {
                var e = new TestEntity
                {
                    Column1 = Guid.NewGuid().ToString(),
                    Column2 = Guid.NewGuid().ToString(),
                    Column3 = Guid.NewGuid().ToString(),
                    Column4 = Guid.NewGuid().ToString(),
                    Column5 = Guid.NewGuid().ToString(),
                    Column6 = Guid.NewGuid().ToString(),
                    Column7 = Guid.NewGuid().ToString(),
                    Column8 = Guid.NewGuid().ToString(),
                    Column9 = Guid.NewGuid().ToString(),
                    Column10 = Guid.NewGuid().ToString(),
                    Column11 = Guid.NewGuid().ToString(),
                    Column12 = Guid.NewGuid().ToString(),
                    Column13 = Guid.NewGuid().ToString(),
                    Column14 = Guid.NewGuid().ToString(),
                    Column15 = Guid.NewGuid().ToString(),
                    Column16 = Guid.NewGuid().ToString()
                };
                retList.Add(e);
            }

            return retList;
        }

        private static void Dapper()
        {
            var sw = Stopwatch.StartNew();
            var entities = PrepareEntities();
            sw.Stop();
            Console.WriteLine($"Preparing entities finished in {sw.ElapsedMilliseconds}ms");

            DapperPlusManager.Entity<TestEntity>()
                .Table("testentity")
                .Identity(x => x.Column1);

            using (var con = new NpgsqlConnection("Host=192.168.1.90;Port=5432;Database=postgres;Username=postgres;Password=mysecretpassword"))
            {
                sw = Stopwatch.StartNew();
                con.BulkInsert<TestEntity>(entities);
                sw.Stop();
                Console.WriteLine($"Inserting entities took {sw.Elapsed.TotalSeconds}s");
            }
        }
    }
}