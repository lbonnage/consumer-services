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
        static IMongoCollection<AnalysisDocument> mongoAnalysisCollection = null;
        static Dictionary<string, IMongoCollection<BsonDocument>> mongoObjectCollections = new Dictionary<string, IMongoCollection<BsonDocument>>();

        static readonly Dictionary<string, string> aliases = new Dictionary<string, string>()
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
                    mongoAnalysisCollection = mongoConsumerDatabase.GetCollection<AnalysisDocument>("analysis");
                    mongoObjectCollections[guid] = mongoConsumerDatabase.GetCollection<BsonDocument>(guid);
                    log.LogInformation("Connected to MongoDB Database");
                }
                catch (Exception e)
                {
                    log.LogError("Error connecting to MongoDB database: " + e.Message);
                    return new InternalServerErrorResult();
                }
            }

            // Retrieve the correct configuration document from the MongoDB database
            log.LogInformation("Attempting to retrieve configuration for ID: " + guid);
            BsonDocument config;
            try
            {
                FilterDefinition<BsonDocument> filter = Builders<BsonDocument>.Filter.Eq("_id", guid);
                config = mongoConfigurationCollection.Find<BsonDocument>(filter).First<BsonDocument>();
            }
            catch (Exception e)
            {
                log.LogError("Error retrieving configuration for data: " + e.Message);
                return new BadRequestObjectResult("Error retrieving configuration for data: " + e.Message);
            }

            log.LogInformation("Retrieved configuration: " + config.GetValue("field_attributes").ToString());

            // Retrieve the correct analysis document from the MongoDB database.  If one doesn't exist for this ID, create one.
            log.LogInformation("Attempting to retrieve analysis for ID: " + guid);
            AnalysisDocument analysis;
            try
            {
                FilterDefinition<AnalysisDocument> filter = Builders<AnalysisDocument>.Filter.Eq("_id", guid);
                analysis = mongoAnalysisCollection.Find<AnalysisDocument>(filter).First<AnalysisDocument>();
            }
            catch (Exception e)
            {
                log.LogError("Error retrieving analysis for data: " + e.Message);
                return new BadRequestObjectResult("Error retrieving configuration for data: " + e.Message);
            }

            // Now you must verify the inputted object data against the configuration
            RobustnessAnalysis(config, document, log);

            // Insert the analysis into the MongoDB analysis collection
            log.LogInformation("Inserting analysis into MongoDB");
            try
            {
                mongoAnalysisCollection.InsertOne(analysis);
            }
            catch (Exception e)
            {
                log.LogError("Failed inserting analysis document into MongoDB database: " + e.Message);
                return new InternalServerErrorResult();
            }

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
        public static void RobustnessAnalysis(BsonDocument config, BsonDocument document, ILogger log)
        {
            log.LogInformation("Beginning robustness analysis of data");

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
                        // Call RobustnessAnalysis recursively on the nested object
                        //RobustnessAnalysis(fieldAttribute.GetValue("field_attributes"), fieldValue.ToBsonDocument(), log);
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

    /**
      * Class used to represent an Analysis of each record type
      * There should be one instance of this in the Analysis database for each config
      */
    public class AnalysisDocument
    {

        [BsonId]
        public string ID { get; set; }

        // the number of records for this ID
        [BsonElement("NumberOfRecords")]
        public int NumberOfRecords { get; set; }

        // invalid values for the type of field, including null
        [BsonElement("BadValueCount")]
        public int BadValueCount { get; set; }

        // data records with missing fields
        [BsonElement("MissingFieldCount")]
        public int MissingFieldCount { get; set; }

        // data records with unexpected fields
        [BsonElement("ExtraFieldCount")]
        public int ExtraFieldCount { get; set; }

        // keep track of one-time total failures
        [BsonElement("OneTimeCommunicationFailureCount")]
        public int OneTimeCommunicationFailureCount { get; set; }

        // keep track of one-time total failures
        [BsonElement("IntermittentCommunicationFailureCount")]
        public int IntermittentCommunicationFailureCount { get; set; }

        // array containing statistical analysis of all relevant data within the document
        public BsonArray StatisticalAnalysis { get; set; }

        public AnalysisDocument(string id)
        {
            this.ID = id;
            this.BadValueCount = 0;
            this.MissingFieldCount = 0;
            this.ExtraFieldCount = 0;
            this.OneTimeCommunicationFailureCount = 0;
            this.IntermittentCommunicationFailureCount = 0;
        }

    }

    //public class Configuration
    //{
    //    [BsonId]
    //    public string Id { get; set; }

    //    public BsonArray field_attributes { get; set; }
    //}

}
