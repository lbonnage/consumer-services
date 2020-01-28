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

namespace configuration
{
    public static class ConfigurationFunction
    {

        // Static MongoDB handles for maintaining a persistent connection across service calls
        static MongoClient mongoClient = null;
        static IMongoDatabase mongoConsumerDatabase = null;
        static IMongoCollection<BsonDocument> mongoConfigurationCollection = null;
        static IMongoCollection<AnalysisDocument> mongoAnalysisCollection = null;

        // Static list of numerical data types for performing numerical analysis on
        static readonly List<string> numericalTypes = new List<string> { "int", "short", "ushort", "long", "uint", "ulong", "float", "double", "decimal" };

        [FunctionName("Configuration")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = "Configuration")] HttpRequest req,
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
            BsonDocument config;
            try
            {
                string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
                object data = JsonConvert.DeserializeObject(requestBody);
                config = BsonDocument.Parse(data.ToString());    // TODO is this the correct way to deserialize and read a BSON object?
            }
            catch (Exception e)
            {
                log.LogError("Error parsing data: " + e.Message);
                return new BadRequestObjectResult("Error parsing data: " + e.Message);
            }

            // In order to preserve the MongoDB client connection across Service calls, perform this check and only connect if necessary
            if (mongoConfigurationCollection is null)
            {
                log.LogInformation("Connecting to MongoDB Database...");
                try
                {
                    mongoClient = new MongoClient(System.Environment.GetEnvironmentVariable("MongoDBAtlasConnectionString"));
                    mongoConsumerDatabase = mongoClient.GetDatabase(System.Environment.GetEnvironmentVariable("DeploymentEnvironment"));
                    mongoConfigurationCollection = mongoConsumerDatabase.GetCollection<BsonDocument>("configurations");
                    mongoAnalysisCollection = mongoConsumerDatabase.GetCollection<AnalysisDocument>("analysis");
                    log.LogInformation("Connected to MongoDB Database");
                }
                catch (Exception e)
                {
                    log.LogError("Error connecting to MongoDB database: " + e.Message);
                    return new InternalServerErrorResult();
                }
            }

            // Construct an AnalysisDocument based on this configuration to place into the analysis collection
            AnalysisDocument analysis = new AnalysisDocument(guid);
            BsonArray statisticalAnalysis;
            ConstructStatisticalAnalysisDocument(out statisticalAnalysis, config);
            analysis.StatisticalAnalysis = statisticalAnalysis;

            // Insert the received configuration into the MongoDB configuration collection
            log.LogInformation("Inserting configuration into MongoDB");
            try
            {
                // Add the GUID specified by the producer as the unique identifier for this document.  We can now link objects to this configuration via this ID.
                config.Add("_id", guid);
                mongoConfigurationCollection.InsertOne(config);
            }
            catch (Exception e)
            {
                log.LogError("Failed inserting configuration into MongoDB database: " + e.Message);
                return new InternalServerErrorResult();
            }

            // Insert the constructed analysis document into the MongoDB analysis collection
            log.LogInformation("Inserting analysis into MongoDB");
            try
            {
                mongoAnalysisCollection.InsertOne(analysis);
            }
            catch (Exception e)
            {
                log.LogError("Failed inserting analysis into MongoDB database: " + e.Message);
                return new InternalServerErrorResult();
            }

            return new OkObjectResult("Succeeded in inserting configuration.");
        }

        /**
         * Construct a document that contains statistical information for all of the fields in the object
         */
        public static void ConstructStatisticalAnalysisDocument(out BsonArray statisticalAnalysis, BsonDocument configuration)
        {
            statisticalAnalysis = new BsonArray();

            foreach (BsonDocument fieldAttribute in configuration.GetValue("field_attributes").AsBsonArray)
            {
                string name = fieldAttribute.GetValue("name").ToString();
                string type = fieldAttribute.GetValue("type").ToString();

                BsonDocument fieldStatistics = new BsonDocument {
                    { "name", name },   // The name of the field
                    { "type", type },
                };

                if (numericalTypes.Contains(type))
                {
                    fieldStatistics.Add("mean", 0);
                    fieldStatistics.Add("standard deviation", 0);
                }
                else if (type == "customobject")
                {
                    BsonArray nestedStatisticalAnalysis;
                    ConstructStatisticalAnalysisDocument(out nestedStatisticalAnalysis, fieldAttribute);
                    fieldStatistics.Add("StatisticalAnalysis", nestedStatisticalAnalysis);
                }

                statisticalAnalysis.Add(fieldStatistics);

            }

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

}
