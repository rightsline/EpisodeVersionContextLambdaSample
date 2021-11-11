using RightsLine.Contracts.RestApi.V4;
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;

namespace EpisodeVersionContextLambdaSample.RightslineAPI
{
    public class RightslineV4 : BaseFacade
    {
        public RightslineV4()
        {
        }

        public async Task<EntityRestModel> Get(string endpoint, long id)
        {
            try
            {
                var response = await this.GatewayApiClient.Request<EntityRestModel>($"{endpoint}/" + id.ToString(), HttpMethod.Get);
                return response;
            }
            catch (Exception ex)
            {
                throw new Exception($"Error on {endpoint} GET: {ex.Message}");
            }
        }

        public async Task<EntityRestModel> Put(string endpoint, long id, string entityJSON)
        {
            try
            {
                var response = await this.GatewayApiClient.Request<EntityRestModel>($"{endpoint}/" + id.ToString(), HttpMethod.Put, entityJSON);

                return response;
            }
            catch(Exception ex)
            {
                throw new Exception($"Error on {endpoint} PUT: {ex.Message}");
            }
        }

        public async Task<int> Post(string endpoint, string entityJSON)
        {
            try
            {
                var response = await this.GatewayApiClient.Request<int>($"{endpoint}", HttpMethod.Post, entityJSON);

                return response;
            }
            catch (Exception ex)
            {
                throw new Exception($"Error on {endpoint} Post: {ex.Message}");
            }
        }

        public async Task<bool> Delete(string endpoint, long id)
        {
            try
            {
                var response = await this.GatewayApiClient.Request<bool>($"{endpoint}/" + id.ToString(), HttpMethod.Delete);

                return response;
            }
            catch (Exception ex)
            {
                throw new Exception($"Error on {endpoint} Delete: {ex.Message}");
            }
        }

        public async Task<EntitySearchResponse> Search(string endpoint, string query)
        {
            try
            {
                return await this.GatewayApiClient.Request<EntitySearchResponse>($"{endpoint}/search", HttpMethod.Post, query);
            }
            catch (Exception ex)
            {
                throw new Exception($"Error {endpoint} SEARCH: {ex.Message}");
            }
        }
    }
}
