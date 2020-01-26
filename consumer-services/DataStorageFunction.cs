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
using MongoDB.Bson.Serialization.Attributes;
using System.Collections.Generic;

namespace data_storage
{

    public static class DataStorageFunction
    {

        // Static MongoDB handles for maintaining a persistent connection across service calls
        static MongoClient mongoClient = null;
        static IMongoDatabase mongoConsumerDatabase = null;
        static IMongoCollection<BsonDocument> mongoConfigurationCollection = null;
        static Dictionary<string, IMongoCollection<BsonDocument>> mongoObjectCollections = new Dictionary<string, IMongoCollection<BsonDocument>>();

        public static readonly Dictionary<string, string> aliases = new Dictionary<string, string>()
        {
            { "String", "string" },
            { "Int32", "int" },
            { "Byte", "byte" },
            { "SByte", "sbyte" },
            { "Int16", "short" },
            { "UInt16", "ushort" },
            { "Int64", "long" },
            { "UInt32", "uint" },
            { "UInt64", "ulong" },
            { "Single", "float" },
            { "Double", "double" },
            { "Decimal", "decimal" },
            { "Object", "object" },
            { "Boolean", "bool" },
            { "Char", "char" },
            { "Document", "object" } // TODO This is for a nested BsonDocument, perhaps should use something besides object
        };

        [FunctionName("DataStorage")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = null)] HttpRequest req,
            ILogger log)
        {

            log.LogInformation("DataStorage function received a request.");

            // Retrieve the GUID to match data to configuration
            string guid = req.Headers["Config-GUID"];
            if (guid is null)
            {
                log.LogError("Error retrieving Config-GUID header");
                return new BadRequestObjectResult("Error retrieving Config-GUID header");
            }
            log.LogInformation("Retrieved ID: " + guid);
            
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
            if (!mongoObjectCollections.ContainsKey(guid) || mongoConfigurationCollection is null)
            {
                log.LogInformation("Connecting to MongoDB Database...");
                try
                {
                    mongoClient = new MongoClient(System.Environment.GetEnvironmentVariable("MongoDBAtlasConnectionString"));
                    mongoConsumerDatabase = mongoClient.GetDatabase(System.Environment.GetEnvironmentVariable("DeploymentEnvironment"));
                    mongoConfigurationCollection = mongoConsumerDatabase.GetCollection<BsonDocument>("configurations");
                    mongoObjectCollections[guid] = mongoConsumerDatabase.GetCollection<BsonDocument>(guid);
                    log.LogInformation("Connected to MongoDB Database");
                }
                catch (Exception e)
                {
                    log.LogError("Error connecting to MongoDB database: " + e.Message);
                    return new InternalServerErrorResult();
                }
            }

            // Retrieve the correct configuration from the MongoDB database and perform the verification steps here
            log.LogInformation("Attempting to retrieve configuration for data");
            BsonDocument config;
            try
            {
                var filter = Builders<BsonDocument>.Filter.Eq("_id", guid);
                config = mongoConfigurationCollection.Find<BsonDocument>(filter).First<BsonDocument>();
            }
            catch (Exception e)
            {
                log.LogError("Error retrieving configuration for data: " + e.Message);
                return new BadRequestObjectResult("Error retrieving configuration for data: " + e.Message);
            }

            log.LogInformation("Retrieved configuration: " + config.GetValue("field_attributes").ToString());

            // Now you must verify the inputted object data against the configuration
            VerifyData(config, document, log);

            // Insert the received object into the MongoDB object collection
            log.LogInformation("Inserting document into MongoDB");
            try
            {
                mongoObjectCollections[guid].InsertOne(document);
            }
            catch (Exception e)
            {
                log.LogError("Failed inserting document into MongoDB database: " + e.Message);
                return new InternalServerErrorResult();
            }

            return new OkObjectResult("Succeeded in inserting document.");

        }

        /**
         * Using the supplied configuration, checks the document for the following:
         * - bad values
         * - missing fields
         * - extra fields
         */
        public static void VerifyData(BsonDocument config, BsonDocument document, ILogger log)
        {
            log.LogInformation("Beginning verification of data");

            int badValueCount = 0;
            int missingFieldsCount = 0;
            int extraFieldsCount = 0;

            //foreach (BsonDocument doc in config.field_attributes)
            //{
            //    System.Diagnostics.Debug.WriteLine(doc.ToString());
            //}

            foreach (BsonDocument fieldAttribute in config.GetValue("field_attributes").AsBsonArray)
            {
                string name = fieldAttribute.GetValue("name").ToString();
                string type = fieldAttribute.GetValue("type").ToString();

                BsonValue fieldValue;

                if (document.TryGetValue(name, out fieldValue))
                {

                    string fieldType = aliases[fieldValue.BsonType.ToString()];

                    // Since we have determined that the field is present, we can now check if the type matches
                    if (fieldType == "object")
                    {
                        // Call VerifyData recursively on the nested object
                        //VerifyData(fieldAttribute.GetValue("field_attributes"), fieldValue.ToBsonDocument(), log);
                    }
                    if (fieldType != type)
                    {
                        badValueCount++;
                        log.LogError("Incorrect value type.  Received " + fieldType + " when we expected " + type + ". Bad value count is now: " +badValueCount);
                    }

                }
                else
                {
                    missingFieldsCount++;
                    log.LogError("Missing field " + type + ".  Missing fields count is now: " + missingFieldsCount);
                }

            }

            // Calculate the number of extra fields in the document
            //int expectedNumberOfFields = config.field_attributes.ToList().Count;


        }

    }

    //public class Configuration
    //{
    //    [BsonId]
    //    public string Id { get; set; }

    //    public BsonArray field_attributes { get; set; }
    //}

}
