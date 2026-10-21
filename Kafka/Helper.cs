using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Kafka.Tool.Cli.Kafka
{
    public class Helper
    {
        static Regex reUnicode = new Regex(@"\\u([0-9a-fA-F]{4})", RegexOptions.Compiled);
        /// <summary>
        /// "Message": "\u6392\u7A0B\u66F4\u65B0" 轉換為 "Message": "排程更新"
        /// 
        /// 注意，如果文字本身，要經過Json反序列化，是不該用這個method的，
        /// Json反序列化本身就會把\uxxxx的轉換成中文，多做這UnicodeToString，反而可能讓有斷行\n的解析失敗
        /// 
        /// </summary>
        /// <param name="s"></param>
        /// <returns></returns>
        public static string UnicodeToString(string s)
        {
            return reUnicode.Replace(s, m =>
            {
                short c;
                if (short.TryParse(m.Groups[1].Value, System.Globalization.NumberStyles.HexNumber, CultureInfo.InvariantCulture, out c))
                {
                    return "" + (char)c;
                }

                return m.Value;
            });
        }

        public static bool TryParseNullableDateTime(string input, out DateTime? result)
        {
            if (DateTime.TryParse(input, out DateTime parsedDateTime))
            {
                result = parsedDateTime;
                return true;
            }
            result = null;
            return false;
        }

        private static readonly Regex createOnRegex = new Regex("\"CreateOn\"\\s*:\\s*\"([^\"]+)\"");

        public static bool TryExtractCreateOn(string json, out DateTime? createOn)
        {
            var match = createOnRegex.Match(json);
            if (match.Success && DateTime.TryParse(match.Groups[1].Value, out DateTime parsedTime))
            {
                createOn = parsedTime;
                return true;
            }
            createOn = null;
            return false;
        }

    }
}
