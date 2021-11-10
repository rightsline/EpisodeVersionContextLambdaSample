using Amazon.Lambda.Core;
using Amazon.Lambda.SNSEvents;
using Newtonsoft.Json;
using RightsLine.Contracts.MessageQueuing.V4.Messages;
using EpisodeVersionContextLambdaSample.Consts;
using EpisodeVersionContextLambdaSample.Models;
using EpisodeVersionContextLambdaSample.RightslineAPI;
using System;
using System.Linq;
using System.Threading.Tasks;
using Amazon.Lambda.Serialization.SystemTextJson;

[assembly: LambdaSerializer(typeof(DefaultLambdaJsonSerializer))]

namespace EpisodeVersionContextLambdaSample
{
    public class Function
    {
        private readonly RightslineV4 _v4 = new RightslineV4();

        public Function()
        {
        }

        public async Task<object> FunctionHandler(SNSEvent snsEvent, ILambdaContext context)
        {
            try
            {
                Console.WriteLine($"SNS Record Count {snsEvent.Records.Count}");

                foreach (var record in snsEvent.Records)
                {

                    //
                    //Sanity checks to confirm message have required attributes used below.
                    //
                    if (!record.Sns.MessageAttributes.ContainsKey(EntityBaseMessageAttributes.Action))
                    {
                        Console.WriteLine($"Message missing action attribute.");
                        continue;
                    }


                    if (!record.Sns.MessageAttributes.ContainsKey(EntityBaseMessageAttributes.CharTypeID))
                    {
                        Console.WriteLine($"Message missing char type id attribute.");
                        continue;
                    }


                    //
                    //The SNS topics are differentiated by char type in the SNS topics name 'ENV-rtl-divNNN-v4-ctNN' 
                    //This makes it unnecessary to extract the char type id from the attributes, however we are
                    //showed how this could be done if a customer were to use a generic lambda for multiple SNS topics.
                    //
                    var charType = (CharTypeID)(Convert.ToInt32(record.Sns.MessageAttributes[EntityBaseMessageAttributes.CharTypeID].Value));
                    switch (charType)
                    {
                        case CharTypeID.CatalogItem:
                            Console.WriteLine($"Catalog-Item message received.");
                            break;
                        case CharTypeID.Relationship:
                            Console.WriteLine($"Relationship message received.");
                            break;
                    }


                    //
                    //act on a particular action as needed
                    //
                    var action = record.Sns.MessageAttributes[EntityBaseMessageAttributes.Action].Value;
                    switch (action)
                    {
                        case EntityBaseMessageActions.EntityActionCreated:
                            Console.WriteLine($"Created");
                            break;

                        case EntityBaseMessageActions.EntityActionUpdated:
                            Console.WriteLine($"Updated");
                            break;

                        case EntityBaseMessageActions.EntityActionDeleted:
                            Console.WriteLine($"Deleted");
                            break;
                    }

                    var messageEntity = JsonConvert.DeserializeObject<ModuleEntityMessage>(record.Sns.Message, Converter.Settings);

                    //SAMPLES
                   
                    //
                    //Example for fetching additional catalog information for a catalog-update message
                    //
                    if (charType == CharTypeID.CatalogItem && action == EntityBaseMessageActions.EntityActionUpdated)
                    {
                        var catalogResult = await this._v4.Get("catalog-item", messageEntity.Entity.EntityId);


                        var catalogSearchPayload = @"{
                                ""start"": 0,
                                ""rows"": 10,
                                ""parentQuery"": { 1:{ ""$eq"":[""recordid"", " + messageEntity.Entity.EntityId + @"]} }
                                }
                                ";

                        var catalogResults = await this._v4.Search("catalog-item", catalogSearchPayload);
                    }


                    //Update a catalog item
                    //A sample cannot be given here as every client has different fields in their unique environment
                    //A recommended approach would be to GET the desired catalog item, modify its value(s), serialize it, and pass to the PUT method
                    //The API call will return the EntityModel with its updated data
                    //var updatedCatalogResult = await this._v4.Put("catalog-item", entityID, entityJSON);

                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[arn:sns {DateTime.Now}] ERROR = {ex.Message} {ex.StackTrace}");
                return new { body = "SNS Event Processing Error at " + DateTime.Now.ToString() + " " + ex.Message, statusCode = 400 };
            }

            return new { body = "SNS Event Processing Completed at " + DateTime.Now.ToString(), statusCode = 200 };
        }
    }
}



