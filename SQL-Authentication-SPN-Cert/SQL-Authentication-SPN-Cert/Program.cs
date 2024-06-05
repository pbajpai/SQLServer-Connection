using System.Data.SqlClient;
using System.Security.Cryptography.X509Certificates;
using Azure.Identity;


namespace SQL_Authentication_Changes
{
    /// <summary>
    /// Interface for creating a database connection.
    /// </summary>
    public interface IDatabaseConnection
    {
        /// <summary>
        /// Creates a SQL connection asynchronously.
        /// </summary>
        /// <returns>A task that represents the asynchronous operation. The task result contains the SQL connection.</returns>
        Task<SqlConnection> CreateConnectionAsync();
    }
    /// <summary>
    /// Class for creating a connection to Azure Synapse.
    /// </summary>
    public class AzureSynapseConnection : IDatabaseConnection
    {
        /// <summary>
        /// Creates a SQL connection to Azure Synapse asynchronously.
        /// </summary>
        /// <returns>A task that represents the asynchronous operation. The task result contains the SQL connection.</returns>
        public async Task<SqlConnection> CreateConnectionAsync()
        {
            // Update the synapse ondemand details
            var builder = new SqlConnectionStringBuilder
            {
                DataSource = "ServerName.sql.azuresynapse.net",
                InitialCatalog = "Database name",
            };

            // Certificate and SPN details. 
            var certificatePath = ""; // Cert pfx file path
            var certificatePassword = ""; // Password for the cert
            var clientId = ""; // client id of the SPN 
            var tenantId = ""; // TenantId of the SPN

            using var certificate = new X509Certificate2(certificatePath, certificatePassword);
            var credential = new ClientCertificateCredential(tenantId, clientId, certificate);
            var token = await credential.GetTokenAsync(new Azure.Core.TokenRequestContext(new[] { "https://sql.azuresynapse.net/.default" }));

            builder.ConnectTimeout = 38000;
            builder.Encrypt = true;
            builder.TrustServerCertificate = false;

            var connection = new SqlConnection(builder.ConnectionString);
            connection.AccessToken = token.Token;
            await connection.OpenAsync();

            return connection;
        }
    }
    /// <summary>
    /// Class for executing a query.
    /// </summary>
    public class QueryExecutor
    {
        private readonly IDatabaseConnection _connection;

        /// <summary>
        /// Initializes a new instance of the <see cref="QueryExecutor"/> class.
        /// </summary>
        /// <param name="connection">The database connection.</param>
        public QueryExecutor(IDatabaseConnection connection)
        {
            _connection = connection;
        }

        /// <summary>
        /// Executes a query asynchronously.
        /// </summary>
        /// <param name="query">The query to execute.</param>
        /// <returns>A task that represents the asynchronous operation.</returns>
        public async Task ExecuteQueryAsync(string query)
        {
            using (var connection = await _connection.CreateConnectionAsync())
            {
                using (SqlCommand command = new SqlCommand(query, connection))
                {
                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            // first column value of the data set
                            Console.WriteLine($"{reader[0]},");
                        }
                    }
                }
            }
        }
    }

    internal class Program
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        static async Task Main(string[] args)
        {
            Console.WriteLine("Hello, SQL connection using Cert!");

            var connection = new AzureSynapseConnection();
            var executor = new QueryExecutor(connection);

            await executor.ExecuteQueryAsync("Select * from [schema].[TableName]");
        }
    }
}


#region SqlAccessScript

//  CREATE USER [SPN Name] FROM EXTERNAL PROVIDER;
//  ALTER ROLE db_datareader ADD MEMBER [SPN Name] ;
//  ALTER ROLE db_datawriter ADD MEMBER [SPN Name] ;


// White list the IP to the SQL Server.

#endregion