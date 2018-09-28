using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Storage_Account_Migrator
    {
    public class Delegates
        {
        public delegate void UpdateLogConsole (string message, bool isError = false);
        }
    class StorageTableMigrator

        {
        private readonly CloudStorageAccount sourceAccount;
        private readonly CloudStorageAccount targetAccount;
        private static Delegates.UpdateLogConsole m_pctCompleteDelegate;
        List<string> s_tables = new List<string> ();

        public StorageTableMigrator (string source, string target, Delegates.UpdateLogConsole pctCompleteDelegate)
            {
            sourceAccount = CloudStorageAccount.Parse (source);

            targetAccount = CloudStorageAccount.Parse (target);

            m_pctCompleteDelegate = pctCompleteDelegate;
            m_pctCompleteDelegate ("StorageAccountMigrator is ready for Migration");
            }

        public async Task Start ()
            {
            var result = await Task.Run (() => ExecuteMigration ());
            m_pctCompleteDelegate (result);
            }

        private async Task<string> ExecuteMigration ()
            {
            var migrateBlobs = CloudConfigurationManager
                                    .GetSetting ("MigrateBlobs") == "true";

            var migrateTables = CloudConfigurationManager
                                    .GetSetting ("MigrateTables") == "true";
            var tasks = new[]
                    {
                    migrateTables
                        ? MigrateTableStorage()
                        : Task.Run(() => { }),
                };

            if (migrateTables)
                return await Task.Run (() => MigrateTableStorage ());
            else
                return "MigrateTables flag is disable";
            }

        private Task<string> MigrateTableStorage ()
            {
            return Task.Run (() =>
            {
                CopyTableStorageFromSource ();
                return "DONE";
            });
            }

        private void CopyTableStorageFromSource ()
            {
            var source = sourceAccount.CreateCloudTableClient ();

            var cloudTables = source.ListTables ()
                .OrderBy (c => c.Name)
                .ToList ();

            foreach (var table in cloudTables)
                {
                if (table.Name.Contains ("AzureWebJobsHostLogs") || table.Name.Contains ("MetricsHourPrimaryTransactions"))
                    continue;
                else
                    CopyTables (table);
                }
            }

        private void CopyTables (CloudTable table)
            {
            var target = targetAccount.CreateCloudTableClient ();

            var targetTable = target.GetTableReference (table.Name);

            targetTable.CreateIfNotExists ();

            targetTable.SetPermissions (table.GetPermissions ());

            m_pctCompleteDelegate ("Created Table Storage :" + table.Name);

            var omit = CloudConfigurationManager
                .GetSetting ("TablesToCreateButNotMigrate")
                .Split (new[] { "|" }, StringSplitOptions.RemoveEmptyEntries);

            if (!omit.Contains (table.Name))
                CopyData (table, targetTable);
            }

        readonly List<ICancellableAsyncResult> queries
            = new List<ICancellableAsyncResult> ();

        readonly Dictionary<string, long> retrieved
            = new Dictionary<string, long> ();

        readonly TableQuery<DynamicTableEntity> query
            = new TableQuery<DynamicTableEntity> ();

        private void CopyData (CloudTable table, CloudTable targetTable)
            {
            ExecuteQuerySegment (table, targetTable);
            }

        private void ExecuteQuerySegment (CloudTable table,
                                            CloudTable targetTable)
            {
            var reqOptions = new TableRequestOptions ();

            var ctx = new OperationContext { ClientRequestID = "StorageMigrator" };

            queries.Add (table.BeginExecuteQuerySegmented (query,
                                                            null,
                                                            reqOptions,
                                                            ctx,
                                                            HandleCompletedQuery (targetTable),
                                                            table));
            }

        private AsyncCallback HandleCompletedQuery (CloudTable targetTable)
            {
            return ar =>
            {
                var cloudTable = ar.AsyncState as CloudTable;

                if (cloudTable == null)
                    return;
                if (!s_tables.Contains (cloudTable.Name))
                    {
                    var response = cloudTable.EndExecuteQuerySegmented<DynamicTableEntity> (ar);

                    var token = response.ContinuationToken;

                    if (token != null)
                        Task.Run (() => ExecuteQuerySegment (cloudTable, targetTable));

                    var retrieved = response.Count ();

                    if (retrieved > 0)
                        Task.Run (() => WriteToTarget (targetTable, response));


                    var recordsRetrieved = retrieved;

                    UpdateCount (cloudTable, recordsRetrieved);
                    string message = $"Table: {cloudTable.Name}, Records: {recordsRetrieved}, Total Records: {this.retrieved[cloudTable.Name]}";
                    m_pctCompleteDelegate (message);


                    object obj = new object ();
                    lock (obj)
                        {
                        s_tables.Add (cloudTable.Name);
                        }
                    }
                else
                    return;
            };
            }

        private void UpdateCount (CloudTable cloudTable, int recordsRetrieved)
            {
            if (!retrieved.ContainsKey (cloudTable.Name))
                retrieved.Add (cloudTable.Name, recordsRetrieved);
            else
                retrieved[cloudTable.Name] += recordsRetrieved;
            }

        //private static void WriteToTarget (CloudTable cloudTable,
        //                                    IEnumerable<DynamicTableEntity> response)
        //    {
        //    var writer = new TableStorageWriter (cloudTable.Name);
        //    foreach (var entity in response)
        //        {
        //        writer.InsertOrReplace (entity);
        //        }
        //    writer.Execute ();
        //    }
        private static void WriteToTarget
            (
            CloudTable cloudTable,
            IEnumerable<DynamicTableEntity> response
            )
            {
            try
                {
                DynamicTableEntity[] dynamicTableEntities = response.ToArray ();
                if (response.Count () < 1)
                    return;

                int remainingCount = response.Count ();
                // Create the batch operation.
                TableBatchOperation batchOperation = new TableBatchOperation ();

                foreach (var entitiesWithSameKey in dynamicTableEntities.GroupBy (x => x.PartitionKey))
                    {
                    int i = 1;
                    foreach (DynamicTableEntity nextEntity in entitiesWithSameKey)
                        {
                        batchOperation.InsertOrReplace (nextEntity);
                        if (i % 50 == 0)
                            {
                            cloudTable.ExecuteBatch (batchOperation);
                            batchOperation.Clear ();
                            }
                        i++;
                        }
                    if (batchOperation != null && batchOperation.Count > 0)
                        {
                        cloudTable.ExecuteBatch (batchOperation);
                        batchOperation.Clear ();
                        }
                    }
                }
            catch (Exception ex)
                {
                m_pctCompleteDelegate (ex.Message, true);
                }
            }
        }
    }
