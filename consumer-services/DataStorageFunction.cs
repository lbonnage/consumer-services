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

namespace data_storage
{

    public static class DataStorageFunction
    {

        static MongoClient mongoClient = null;
        static IMongoDatabase mongoDataStorageDatabase = null;
        static IMongoCollection<BsonDocument> mongoObjectCollection = null;

        [FunctionName("DataStorage")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = null)] HttpRequest req,
            ILogger log)
        {

            log.LogInformation("DataStorage function received a request.");

            // Parse the message body
            BsonDocument document;
            try
            {
                string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
                object data = JsonConvert.DeserializeObject(requestBody);
                document = BsonDocument.Parse(data.ToString());    // TODO is this the correct way to deserialize and read a BSON object?
            } catch (Exception e)
            {
                log.LogError("Error parsing data: " + e.Message);
                return new BadRequestObjectResult("Error parsing data: " + e.Message);
            }

            // In order to preserve the MongoDB client connection across Function calls, perform this check and only connect if necessary
            if (mongoObjectCollection is null)
            {
                try
                {
                    mongoClient = new MongoClient(System.Environment.GetEnvironmentVariable("MongoDBAtlasConnectionString"));
                    mongoDataStorageDatabase = mongoClient.GetDatabase("DataStorage");
                    mongoObjectCollection = mongoDataStorageDatabase.GetCollection<BsonDocument>("objects");
                    log.LogInformation("Connected to MongoDB Database");
                } catch (Exception e)
                {
                    log.LogError("Error connecting to MongoDB database: " + e.Message);
                    return new InternalServerErrorResult();
                }
            }

            // Insert the received object into the MongoDB object collection
            log.LogInformation("Inserting document");
            try
            {
                mongoObjectCollection.InsertOne(document);
            } catch (Exception e)
            {
                log.LogError("Failed inserting document into MongoDB database: " + e.Message);
                return new InternalServerErrorResult();
            }

            return new OkObjectResult("Succeeded in inserting document.");

        }
    }
}
