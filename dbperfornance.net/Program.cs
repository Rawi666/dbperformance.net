using Npgsql;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Transactions;
using Z.Dapper.Plus;

namespace dbperfornance.net
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            //Dapper();
            PgsqlCopy();
        }

        private static int numberOfEntities = 100_000;

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

            using (var tx = new TransactionScope())
            {
                using (var con = new NpgsqlConnection("Host=192.168.1.90;Port=5432;Database=postgres;Username=postgres;Password=mysecretpassword"))
                {
                    con.Open();

                    sw = Stopwatch.StartNew();
                    con.BulkInsert<TestEntity>(entities);
                    sw.Stop();
                    Console.WriteLine($"Inserting entities took {sw.Elapsed.TotalSeconds}s");
                }

                tx.Complete();
            }
        }

        private static void PgsqlCopy()
        {
            var sw = Stopwatch.StartNew();
            var entities = PrepareEntities();
            sw.Stop();
            Console.WriteLine($"Preparing entities finished in {sw.ElapsedMilliseconds}ms");

            using (var con = new NpgsqlConnection("Host=192.168.1.90;Port=5432;Database=postgres;Username=postgres;Password=mysecretpassword"))
            {
                var sql = "COPY testentity (Column1,Column2,Column3,Column4,Column5,Column6,Column7,Column8,Column9,Column10,Column11,Column12,Column13,Column14,Column15,Column16) FROM STDIN (FORMAT BINARY)";
                con.Open();
                sw = Stopwatch.StartNew();
                using (var tx = con.BeginTransaction())
                {
                    using (var writer = con.BeginBinaryImport(sql))
                    {
                        foreach (var e in entities)
                        {
                            writer.StartRow();
                            writer.Write(e.Column1);
                            writer.Write(e.Column2);
                            writer.Write(e.Column3);
                            writer.Write(e.Column4);
                            writer.Write(e.Column5);
                            writer.Write(e.Column6);
                            writer.Write(e.Column7);
                            writer.Write(e.Column8);
                            writer.Write(e.Column9);
                            writer.Write(e.Column10);
                            writer.Write(e.Column11);
                            writer.Write(e.Column12);
                            writer.Write(e.Column13);
                            writer.Write(e.Column14);
                            writer.Write(e.Column15);
                            writer.Write(e.Column16);
                        }
                        writer.Complete();
                    }
                    tx.Commit();
                }
                Console.WriteLine($"Inserting entities took {sw.Elapsed.TotalSeconds}s");
            }
        }
    }
}