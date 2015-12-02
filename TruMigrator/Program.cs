namespace TruMigrator
{
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Data.SqlClient;
    using System.IO;
    using System.Linq;
    using System.Net.Http.Headers;
    using System.Text;
    using System.Threading.Tasks;
    using Dapper;

    class Program
    {
        static void Main(string[] args)
        {
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine("-----------------------------------------------------------------------------");
            Console.WriteLine("===================================TruMigrator===============================");
            Console.WriteLine("-----------------------------------------------------------------------------");
            Console.ForegroundColor = ConsoleColor.White;
            string sqlConnectionString = ConfigurationManager.AppSettings["ConnectionString"];

            List<string> migrationFiles;
            try
            {
                migrationFiles = GetMigrationFileNames().ToList();
            }
            catch (Exception)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("ERROR: Path '{0}' to migration directory not found", ConfigurationManager.AppSettings["MigrationDirectory"]);
                Console.ReadKey();
                return;
            }

            Console.WriteLine("[+]Using SQL Connection String: '{0}'", sqlConnectionString);
            using (var connection = new SqlConnection(sqlConnectionString))
            {
                try
                {
                    CreateMigrationTableIfNotExist(connection, migrationFiles);
                }
                catch (Exception e)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("ERROR: {0}", e.Message);
                    Console.ReadKey();
                    return;
                }

                Console.WriteLine("[+]Beggining transaction");
                int currentMigration;
                try
                {
                    currentMigration = GetFirstNotAppliedMigration(connection, migrationFiles);
                }
                catch (Exception)
                {
                    return;
                }

                for (; currentMigration < migrationFiles.Count; currentMigration++)
                {
                    try
                    {
                        ApplyMigration(migrationFiles[currentMigration], connection);
                    }
                    catch (Exception e)
                    {
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine("ERROR: {0}", e.Message);
                        Console.ReadKey();
                        return;
                    }
                }

                Console.WriteLine("[+] Commiting transaction");
                Console.WriteLine("[+] Task completed");
                Console.ReadKey();
            }
        }

        private static void ApplyMigration(string migrationFile, SqlConnection connection)
        {
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine("-----------------------------------------------------------------------------");
            Console.WriteLine(migrationFile);
            Console.WriteLine("-----------------------------------------------------------------------------");
            Console.ForegroundColor = ConsoleColor.White;

            string script = File.ReadAllText(Path.Combine(ConfigurationManager.AppSettings["MigrationDirectory"], migrationFile));
            Console.WriteLine("Execute SQL Script");
            Console.WriteLine(script);
            Console.WriteLine();
            connection.Execute(script);

            AddMigrationToTable(connection, migrationFile);
            Console.WriteLine("[+] {0} migrated",
                Path.GetFileNameWithoutExtension(migrationFile));
        }

        private static int GetFirstNotAppliedMigration(SqlConnection connection, List<string> migrationFiles)
        {
            int currentMigration = 0;
            var appliedMigration =
                connection.Query<Migration>("SELECT * FROM [dbo].[__TruMigrationHistory]")
                    .OrderBy(x => x.MigrationId)
                    .ToList();

            foreach (var migration in appliedMigration)
            {
                var migrationTableFullName = Path.ChangeExtension(migration.MigrationId, ".sql");
                if (migrationTableFullName != migrationFiles[currentMigration])
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("ERROR: {0} migration filename do not match migration history",
                        currentMigration);
                    Console.ReadKey();
                    throw new Exception();
                }
                currentMigration++;
            }
            return currentMigration;
        }

        private static void CreateMigrationTableIfNotExist(SqlConnection connection, List<string> migrationFiles)
        {
            var checkTableExistScript =
                "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = '__TruMigrationHistory'";
            var isTableExists = connection.Query(checkTableExistScript).Count();
            if (isTableExists == 0)
            {
                Console.WriteLine(
                    "There is no migration history. Please, write how many migrations were applied already.");
                var count = Convert.ToInt32(Console.ReadLine());
                connection.Execute(
                    "CREATE	TABLE __TruMigrationHistory (MigrationId varchar(150) NOT NULL PRIMARY KEY, CreatedOn DateTime);");
                for (int i = 0; i < count; i++)
                {
                    AddMigrationToTable(connection, migrationFiles[i]);
                }
            }
        }

        private static void AddMigrationToTable(SqlConnection connection, string filename)
        {
            string sql = @"INSERT INTO [dbo].[__TruMigrationHistory] (MigrationId, CreatedOn) VALUES (@MigrationId, @CreatedOn);";

            connection.Execute(sql, new
            {
                MigrationId = Path.GetFileNameWithoutExtension(Path.GetFileNameWithoutExtension(filename)),
                CreatedOn = DateTime.Now
            });
        }

        private static IEnumerable<string> GetMigrationFileNames()
        {
            var fullPath = Directory.GetFiles(ConfigurationManager.AppSettings["MigrationDirectory"], "*.sql");
            return fullPath.Select(Path.GetFileName).OrderBy(f => f);
        } 
    }
}
