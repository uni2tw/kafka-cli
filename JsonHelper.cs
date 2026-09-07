using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace Kafka.Tool.Cli
{
    public static class JsonHelper
    {
        static JsonSerializerOptions DefaultSerializeOption = new JsonSerializerOptions
        {
            Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
            WriteIndented = false
        };
        public static string Serialize(object obj)
        {
            return JsonSerializer.Serialize(obj, DefaultSerializeOption);
        }

        public static T? Deserialize<T>(string str)
        {
            if (string.IsNullOrEmpty(str))
            {
                return default(T);
            }
            return JsonSerializer.Deserialize<T>(str, DefaultSerializeOption);
        }

        public static object Deserialize(Type type, string str)
        {
            if (string.IsNullOrEmpty(str))
            {
                return null;
            }
            return JsonSerializer.Deserialize(str, type);
        }

        public static string FindByJsonPath(JsonElement element, string path)
        {
            string[] pathSegments = path.Trim('/').Split('/');
            JsonElement currentElement = element;

            foreach (string segment in pathSegments)
            {
                if (!currentElement.TryGetProperty(segment, out currentElement))
                {
                    return null;
                }
            }

            return currentElement.ToString();
        }
    }
}
