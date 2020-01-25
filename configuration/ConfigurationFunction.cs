using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using MongoDB.Driver;
using MongoDB.Bson;
using System.Web.Http;

namespace configuration
{
    public static class ConfigurationFunction
    {

        // Static MongoDB handles for maintaining a persistent connection across service calls
        static MongoClient mongoClient = null;
        static IMongoDatabase mongoDataStorageDatabase = null;
        static IMongoCollection<BsonDocument> mongoObjectCollection = null;

        [FunctionName("Configuration")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("Configuration function received a request.");

            // Retrieve the GUID generated for this specific configuration
            string guid = req.Headers["Config-GUID"];
            if (guid is null)
            {
                log.LogError("Error retrieving Config-GUID header");
                return new BadRequestObjectResult("Error retrieving Config-GUID header");
            }

            // Parse the message body
            BsonDocument document;
            try
            {
                string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
                object data = JsonConvert.DeserializeObject(requestBody);
                document = BsonDocument.Parse(data.ToString());    // TODO is this the correct way to deserialize and read a BSON object?
            }
            catch (Exception e)
            {
                log.LogError("Error parsing data: " + e.Message);
                return new BadRequestObjectResult("Error parsing data: " + e.Message);
            }

            // In order to preserve the MongoDB client connection across Service calls, perform this check and only connect if necessary
            if (mongoObjectCollection is null)
            {
                log.LogInformation("Connecting to MongoDB Database...");
                try
                {
                    mongoClient = new MongoClient(System.Environment.GetEnvironmentVariable("MongoDBAtlasConnectionString"));
                    mongoDataStorageDatabase = mongoClient.GetDatabase(System.Environment.GetEnvironmentVariable("DeploymentEnvironment"));
                    mongoObjectCollection = mongoDataStorageDatabase.GetCollection<BsonDocument>("configurations");
                    log.LogInformation("Connected to MongoDB Database");
                }
                catch (Exception e)
                {
                    log.LogError("Error connecting to MongoDB database: " + e.Message);
                    return new InternalServerErrorResult();
                }
            }

            // Insert the received configuration into the MongoDB configuration collection
            log.LogInformation("Inserting configuration into MongoDB");
            try
            {
                // Add the GUID specified by the producer as the unique identifier for this document.  We can now link objects to this configuration via this ID.
                document.Add("_id", guid);
                mongoObjectCollection.InsertOne(document);
            }
            catch (Exception e)
            {
                log.LogError("Failed inserting document into MongoDB database: " + e.Message);
                return new InternalServerErrorResult();
            }

            return new OkObjectResult("Succeeded in inserting configuration.");
        }
    }
}
