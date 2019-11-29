using Elasticsearch.Net;
using Nest;
using Nest.JsonNetSerializer;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ElasticSearch.NestClient
{
    public class NestClient
    {
        private ElasticClient _client;
        public NestClient(string uri)
        {
            var pool = new SingleNodeConnectionPool(new Uri(uri));
            var connectionSettings = new ConnectionSettings(pool, sourceSerializer: JsonNetSerializer.Default);

            _client = new ElasticClient(connectionSettings);
        }

        /// <summary>
        /// 基本查詢
        /// </summary>
        /// <param name="type">log type ex:login</param>
        /// <param name="startTime">參考欄位為@timestamp</param>
        /// <param name="endTime">參考欄位為@timestamp</param>
        /// <returns></returns>
        public async Task<List<T>> SearchAsync<T>(string type, DateTime startTime, DateTime endTime) where T : class
        {
            startTime = DateTime.SpecifyKind(startTime, DateTimeKind.Utc);
            endTime = DateTime.SpecifyKind(endTime, DateTimeKind.Utc);

            Indices indices = ConstituteIndices(type, startTime, endTime)?.ToArray();

            if (indices != null)
            {
                var response = await _client.SearchAsync<T>(s => s
                        .IgnoreUnavailable()
                        .Index(indices)
                        .Size(10000)
                        .Scroll("2m")
                        .Query(q => q
                            .DateRange(r => r
                                .Field("@timestamp")
                                .GreaterThanOrEquals(DateMath.Anchored(startTime))
                                .LessThan(DateMath.Anchored(endTime))
                            )
                        )
                    );

                var data = await GetScrollData(response); ;
                return data;
            }

            return null;
        }

        /// <summary>
        /// 多重條件查詢
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="type"></param>
        /// <param name="fields"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <returns></returns>
        public async Task<List<T>> SearchByMatchTermsAsync<T>(string type, Dictionary<string, string[]> fields, DateTime startTime, DateTime endTime) where T : class
        {
            startTime = DateTime.SpecifyKind(startTime, DateTimeKind.Utc);
            endTime = DateTime.SpecifyKind(endTime, DateTimeKind.Utc);

            Indices indices = ConstituteIndices(type, startTime, endTime)?.ToArray();

            if (indices != null)
            {
                var response = await _client.SearchAsync<T>(s => s
                        .IgnoreUnavailable()
                        .Index(indices)
                        .Size(10000)
                        .Scroll("2m")
                        .Query(q =>
                        {
                            var query = q
                            .DateRange(r => r
                                .Field("@timestamp")
                                .GreaterThanOrEquals(DateMath.Anchored(startTime))
                                .LessThan(DateMath.Anchored(endTime))
                            );

                            if (fields != null)
                            {
                                foreach (var f in fields)
                                {
                                    query = query &&
                                      q.Terms(c => c
                                          .Field(f.Key)
                                          .Terms(f.Value)
                                      );
                                }
                            }

                            return query;
                        })
                    );

                var data = await GetScrollData(response);
                return data;
            }

            return null;
        }

        public async Task<List<T>> SearchByGreaterThanAsync<T>(string type, DateTime startTime, DateTime endTime, string field, int lowerBound) where T : class
        {
            startTime = DateTime.SpecifyKind(startTime, DateTimeKind.Utc);
            endTime = DateTime.SpecifyKind(endTime, DateTimeKind.Utc);

            Indices indices = ConstituteIndices(type, startTime, endTime)?.ToArray();

            if (indices != null)
            {
                var response = await _client.SearchAsync<T>(s => s
                        .IgnoreUnavailable()
                        .Index(indices)
                        .Size(10000)
                        .Scroll("2m")
                        .Query(q =>
                        {
                            var query = q
                            .DateRange(r => r
                                .Field("@timestamp")
                                .GreaterThanOrEquals(DateMath.Anchored(startTime))
                                .LessThan(DateMath.Anchored(endTime))
                            );

                            query = q
                            .Range(c => c
                            .Field(field)
                            .GreaterThanOrEquals(lowerBound)
                            );

                            return query;
                        })
                    );

                var data = await GetScrollData(response);
                return data;
            }

            return null;
        }

        public async Task<List<T>> SearchByMatchTermsGreaterThanAsync<T>(string type, DateTime startTime, DateTime endTime, string field, int lowerBound, Dictionary<string, string[]> fields) where T : class
        {
            startTime = DateTime.SpecifyKind(startTime, DateTimeKind.Utc);
            endTime = DateTime.SpecifyKind(endTime, DateTimeKind.Utc);

            Indices indices = ConstituteIndices(type, startTime, endTime)?.ToArray();

            if (indices != null)
            {
                var response = await _client.SearchAsync<T>(s => s
                        .IgnoreUnavailable()
                        .Index(indices)
                        .Size(10000)
                        .Scroll("2m")
                        .Query(q =>
                        {
                            var query = q
                            .DateRange(r => r
                                .Field("@timestamp")
                                .GreaterThanOrEquals(DateMath.Anchored(startTime))
                                .LessThan(DateMath.Anchored(endTime))
                            );

                            query = q
                            .Range(c => c
                            .Field(field)
                            .GreaterThanOrEquals(lowerBound)
                            );

                            if (fields != null)
                            {
                                foreach (var f in fields)
                                {
                                    query = query &&
                                      q.Terms(c => c
                                          .Field(f.Key)
                                          .Terms(f.Value)
                                      );
                                }
                            }

                            return query;
                        })
                    );

                var data = await GetScrollData(response);
                return data;
            }

            return null;
        }

        /// <summary>
        /// 取得Scroll response的所有資料
        /// </summary>
        /// <param name="response"></param>
        /// <returns></returns>
        private async Task<List<T>> GetScrollData<T>(ISearchResponse<T> response) where T : class
        {
            if (response != null)
            {
                List<T> data = new List<T>();

                if (!response.IsValid)
                    throw new Exception(response.ServerError.Error.Reason);

                if (string.IsNullOrEmpty(response.ScrollId))
                {
                    data.AddRange(response.Documents);
                    return data;
                }


                var scrollId = response.ScrollId;

                while (response.Documents.Any())
                {
                    data.AddRange(response.Documents);

                    response = await _client.ScrollAsync<T>("2m", scrollId);

                    if (response.IsValid)
                    {
                        scrollId = response.ScrollId;
                    }
                }

                return data;
            }

            return null;
        }

        /// <summary>
        /// 根據type及起訖時間組合indices
        /// </summary>
        /// <param name="type"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <returns></returns>
        private List<string> ConstituteIndices(string type, DateTime startTime, DateTime endTime)
        {
            if (type != null && endTime > startTime)
            {
                List<string> indices = new List<string>();

                int months = (endTime.Year - startTime.Year) * 12 + (endTime.Month - startTime.Month);
                for (int i = 0; i <= months; i++)
                {
                    var time = startTime.Date.AddMonths(i);

                    type = type.ToLower();
                    var year = time.Year;
                    var month = time.Month.ToString("00");

                    var index = $"{type}_{year}.{month}";

                    //Search時有IgnoreUnavailable可排除不存在的index
                    //所以這邊改為不檢查全加
                    indices.Add(index);

                    Console.WriteLine(index);
                }

                return indices;
            }
            else
            {
                return null;
            }
        }
    }
}
