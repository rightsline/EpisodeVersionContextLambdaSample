using System;
using System.Collections.Generic;
using System.Text;

namespace EpisodeVersionContextLambdaSample.RightslineAPI.Responses
{
    public class TemporaryCredentialsResponse
    {
        public string AccessKey { get; set; }
        public string SecretKey { get; set; }
        public string SessionToken { get; set; }
        public DateTime Expiration { get; set; }
    }
}
