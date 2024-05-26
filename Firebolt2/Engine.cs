using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace Firebolt2
{
    public class Engine
    {
        [JsonPropertyName("engineUrl")]
        public string EngineUrl { get; set; }
        
    }
}
