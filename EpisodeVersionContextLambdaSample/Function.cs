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
using System.Collections.Generic;
using RightsLine.Contracts.RestApi.V4;

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

				foreach(var record in snsEvent.Records)
				{

					//
					//Sanity checks to confirm message have required attributes used below.
					//
					if(!record.Sns.MessageAttributes.ContainsKey(EntityBaseMessageAttributes.Action))
					{
						Console.WriteLine($"Message missing action attribute.");
						continue;
					}


					if(!record.Sns.MessageAttributes.ContainsKey(EntityBaseMessageAttributes.CharTypeID))
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
					switch(charType)
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
					switch(action)
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

					//
					//Example for fetching additional catalog information for a catalog-update message
					//
					if(charType == CharTypeID.CatalogItem && action == EntityBaseMessageActions.EntityActionUpdated)
					{
						var messageEntity = JsonConvert.DeserializeObject<ModuleEntityMessage>(record.Sns.Message, Converter.Settings);
						var catalogResult = await _v4.Get("catalog-item", messageEntity.Entity.EntityId);
						ProcessCatalogUpdated(catalogResult);
					}
					else if( charType == CharTypeID.Relationship )
					{
						var messageRelationship = JsonConvert.DeserializeObject<RelationshipMessage>(record.Sns.Message, Converter.Settings);
						var parent = await _v4.Get("catalog-item", messageRelationship.ParentEntity.EntityId);
						ProcessCatalogUpdated(parent);
					}

				}
			}
			catch(Exception ex)
			{
				Console.WriteLine($"[arn:sns {DateTime.Now}] ERROR = {ex.Message} {ex.StackTrace}");
				return new { body = "SNS Event Processing Error at " + DateTime.Now.ToString() + " " + ex.Message, statusCode = 400 };
			}

			return new { body = "SNS Event Processing Completed at " + DateTime.Now.ToString(), statusCode = 200 };
		}

		private async void ProcessCatalogUpdated(EntityRestModel catalog)
		{
			if(catalog.Template.TemplateId == Consts.Templates.Episode)
			{
				var season = await GetParent(catalog.Id.Value);
				var series = await GetParent(season.Id.Value);
				var versions = await GetChildren(catalog.Id.Value);
				foreach(var version in versions) // could be made to use the bulk endpoints
				{
					var v = await this._v4.Get("catalog-item", version.Id.Value);

					// TODO: add conditional here to check if new values are different than current values.  if values are the same, no update needed.
					v.Characteristics[Consts.Characteristics.ParentEpisode] = new List<CharDataRestModel>() { new CharDataRestModel() { Value = catalog.Title } };
					v.Characteristics[Consts.Characteristics.ParentSeason] = new List<CharDataRestModel>() { new CharDataRestModel() { Value = season.Title } };
					v.Characteristics[Consts.Characteristics.ParentSeries] = new List<CharDataRestModel>() { new CharDataRestModel() { Value = series.Title } };
					
					var response = await this._v4.Put("catalog-item", v.Id.Value, JsonConvert.SerializeObject(v));
				}
			}
			else if(catalog.Template.TemplateId == Consts.Templates.Season ||
				catalog.Template.TemplateId == Consts.Templates.Series)
			{
				var children = await GetChildren(catalog.Id.Value);
				foreach(var item in children)
				{
					ProcessCatalogUpdated(item);//ideally this would be a separate lambda execution
				}
			}
		}

		private async Task<IEnumerable<EntityRestModel>> GetChildren(int entityId)
		{
			List<EntityRestModel> results = new List<EntityRestModel>();
			int start = 0, numFound;
            
			do
			{
				var catalogSearchPayload = @"{
                                ""start"": " + start + @",
                                ""rows"": 100,
                                ""parentQuery"": { 1:{ ""$eq"":[""recordid"", " + entityId + @"]} }
                                }";

				var response = await this._v4.Search("catalog-item", catalogSearchPayload);

				results.AddRange(response.Entities);
				numFound = response.NumFound;
				start += 100;
			}
			while (start < numFound);

			return results;
		}

		private async Task<EntityRestModel> GetParent(int entityId)
		{
			var catalogSearchPayload = @"{
                                ""start"": 0,
                                ""rows"": 1,
                                ""childQuery"": { 1:{ ""$eq"":[""recordid"", " + entityId + @"]} }
                                }
                                ";

			var response = await this._v4.Search("catalog-item", catalogSearchPayload);
			return response.Entities.First(); //assumes catalog items have a single parent
		}
	}
}



