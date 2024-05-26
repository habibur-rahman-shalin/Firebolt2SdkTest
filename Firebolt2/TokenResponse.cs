using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace Firebolt2
{
    public class TokenResponse
    {
        [JsonPropertyName("access_token")]
        public string AccessToken { get; set; }
        
        [JsonPropertyName("token_type")]
        public string TokenType { get; set; }

        [JsonPropertyName("Access_token")]
        public int expires_in { get; set; }
    }
}
