using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Messaging;
using System.Transactions;

using TweetSharp;
using Newtonsoft.Json;
using System.Threading;

namespace TweetProducer
{
    class Program
    {
        static void Main(string[] args)
        {
            var lastIdByUser = new Dictionary<string, long?>();
            while (true)
            {
                ProduceTweet(args, lastIdByUser);
                Console.WriteLine("Sleeping...");
                Thread.Sleep(5000);
            }
        }

        private static void ProduceTweet(string[] args, Dictionary<string, long?> lastIdByUser)
        {
            string pathFile = ConfigurationManager.AppSettings["PathFile"];
            string queueName = ConfigurationManager.AppSettings["QueueName"];

            bool isFirstUser = true;
            long? lastId = null;
            bool isWritten2File = false;            
            // for each param specified on the command line
            for (int i = 0; i < args.Length; i++)
            {                    
                if (args[i] == "-file")
                {
                    isWritten2File = true;
                }
                else if (args[i] == "-lastId")
                {
                    // get last id
                    if (isWritten2File)
                        lastId = Get_last_tweet_id_by_file(pathFile);
                }
            }

            string userNames = ConfigurationManager.AppSettings["UserNames"];
            var userShuffle = Shuffle(userNames.Split(new char[] {'|'}));
            foreach (string userName in userShuffle)
            {
                SendTweetsByUser(lastIdByUser, userName, lastId, queueName, null);
            }

            string topicsQuery = ConfigurationManager.AppSettings["TopicsQuery"];
            var topicsShuffle = Shuffle(topicsQuery.Split(new char[] { '|' }));
            foreach (string topicQuery in topicsShuffle)
            {
                Console.WriteLine();
                Console.WriteLine(string.Format("{0} - Getting tweets by search \"{1}\"...",
                                                                    DateTime.Now.ToString("s"), topicQuery));                
                var tweets = Get_tweets_from_search(topicQuery, null);

                if (tweets != null && tweets.Any())
                {
                    var tweetsGroupByUser = tweets.GroupBy(t => t.User.Id);
                    foreach (var tweetsByUser in tweetsGroupByUser)
                    {
                        var lastTweet = tweetsByUser.OrderBy(t => t.Id).Last();
                        var userName = lastTweet.User.ScreenName;

                        if (lastIdByUser.TryGetValue(userName, out lastId))
                        {
                            if (lastId >= lastTweet.Id)
                            {
                                //SendTweetsByFriendship(lastIdByUser, userName, topicQuery, lastId, queueName, 7);
                                continue;
                            }
                        }
                        else
                        {
                            lastIdByUser[userName] = lastTweet.Id;

                            Console.WriteLine();
                            Console.WriteLine(string.Format("{0} - Sending tweets from @{1}...",
                                DateTime.Now.ToString("s"), userName));
                            SendTweets(queueName, tweetsByUser);
                        }
                        
                        SendTweetsByUser(lastIdByUser, userName, lastId, queueName, topicQuery);
                    }
                }
                
            }
            
        }

        private static void SendTweetsByFriendship(Dictionary<string, long?> lastIdByUser, string userName, string topicQuery, long? lastId,
                                                   string queueName, int recurCount)
        {
            if (recurCount <= 0)
                return;

            var tweetFriends = Get_tweets_by_user_friends(userName);
            if (tweetFriends != null)
            {
                foreach (var tweetFriend in tweetFriends.Where(t => t.Status != null))
                {
                    List<string> hashtags;
                    List<string> mentions;
                    List<string> urls;
                    GetEntityInfos(tweetFriend.Status, out hashtags, out mentions, out urls);
                    if (hashtags.Any())
                    {
                        foreach (var topic in topicQuery.Split(new char[] {'+'}))
                        {
                            foreach (var hashtag in hashtags)
                            {
                                string ht = hashtag.Trim(new char[] { '#', '\"' });
                                if (ht.Contains(topic.Trim()))
                                    SendTweetsByUser(lastIdByUser, tweetFriend.ScreenName, lastId, queueName, null);
                            }
                        }
                    }
                    if (mentions.Any())
                    {
                        foreach (var mention in mentions)
                        {
                            string mentionName = mention.Trim(new char[] {'@', '\"'});
                            SendTweetsByFriendship(lastIdByUser, mentionName, topicQuery, lastId, queueName, recurCount-1);
                        }
                    }
                }
            }
        }

        private static void SendTweetsByUser(Dictionary<string, long?> lastIdByUser, string userName, long? lastId,
            string queueName, string topicQuery)
        {
            Console.WriteLine(string.Format("{0} - Getting tweets from @{1} since Id={2}...", 
                DateTime.Now.ToString("s"), userName, lastId));

            IEnumerable<TwitterStatus> tweets;
            tweets = Get_tweets_on_specified_user_timeline(userName, lastId);
            if (tweets != null && tweets.Any())
            {
                if (string.IsNullOrEmpty(topicQuery))
                {
                    lastId = tweets.OrderBy(t => t.Id).Last().Id;
                    lastIdByUser[userName] = lastId.Value;
                    Console.WriteLine();
                    Console.WriteLine(string.Format("{0} - Sending tweets from @{1}...",
                                                    DateTime.Now.ToString("s"), userName));
                    SendTweets(queueName, tweets);
                }
                else
                {
                    var topicTweets = new List<TwitterStatus>();
                    foreach (var tweet in tweets)
                    {
                        List<string> hashtags;
                        List<string> mentions;
                        List<string> urls;
                        GetEntityInfos(tweet, out hashtags, out mentions, out urls);
                        foreach (var topic in topicQuery.Split(new char[] { '+' }))
                        {
                            foreach (var hashtag in hashtags)
                            {
                                string ht = hashtag.Trim(new char[] { '#', '\"' });
                                if (ht.Contains(topic.Trim(new char[] { '#', '\"', ' '})))
                                    topicTweets.Add(tweet);
                            }
                        }
                    }
                    if (topicTweets.Any())
                    {
                        lastId = topicTweets.OrderBy(t => t.Id).Last().Id;
                        lastIdByUser[userName] = lastId.Value;
                        Console.WriteLine();
                        Console.WriteLine(string.Format("{0} - Sending tweets from @{1}...",
                                                        DateTime.Now.ToString("s"), userName));
                        SendTweets(queueName, topicTweets);                        
                    }
                }

            }
        }

        private static IEnumerable<TwitterUser> Get_tweets_by_user_friends(string userName)
        {
            try
            {
                var serviceHelper = new TwitterServiceHelper();
                var service = serviceHelper.GetAuthenticatedService();

                return service.ListFriends(
                    new ListFriendsOptions()
                        {
                            ScreenName = userName
                        });
            }
            catch (Exception exc)
            {
                Console.WriteLine("Caught  exception: {0} - {1}", exc.Source, exc.Message);
                return null;
            }
        }

        private static long? Get_last_tweet_id_by_file(string pathFile)
        {
            long? lastId = null;

            var di = new System.IO.DirectoryInfo(pathFile);
            if (di.Exists)
            {
                string pathFileName = System.IO.Path.Combine(pathFile, "tweets.json");

                if (System.IO.File.Exists(pathFileName))
                {
                    var sr = new System.IO.StreamReader(pathFileName);
                    var jtr = new JsonTextReader(sr);
                    try
                    {
                        while (jtr.Read())
                        {
                            var tweetToken = jtr.Value;
                            if (tweetToken != null)
                            {
                                string tokenName = tweetToken as string;
                                if (tokenName == "id")
                                {
                                    jtr.Read();
                                    lastId = Convert.ToInt64(jtr.Value);
                                    break;
                                }
                            }
                        }
                    }
                    catch (Exception exc)
                    {
                        Console.Error.WriteLine(exc.Message);
                    }
                    finally
                    {
                        jtr.Close();
                    }
                }
            }
            return lastId;
        }

        private static void WriteTweets(string pathFile, bool isFirstUser, IEnumerable<TwitterStatus> tweets)
        {
            string pathFileName = System.IO.Path.Combine(pathFile, "tweets.json");
            var sw = new System.IO.StreamWriter(pathFileName, !isFirstUser);
            var jtw = new JsonTextWriter(sw);

            try
            {
                var di = new System.IO.DirectoryInfo(pathFile);
                if (di.Exists)
                {


                    int count = 0;
                    foreach (var tweet in tweets)
                    {
                        if (string.IsNullOrEmpty(tweet.RawSource)) continue;
                        count++;
                        Console.WriteLine("{0} said '{1}' on {2}", tweet.User.ScreenName, tweet.Text, tweet.CreatedDate.ToString());
                        jtw.WriteRaw(tweet.RawSource);
                    }
                    jtw.Flush();
                    Console.WriteLine("{0} line(s) written.", count);
                }
            }
            catch (Exception exc)
            {
                Console.Error.WriteLine(exc.Message);
            }
            finally
            {
                jtw.Close();
            }
        }

        private static void SendTweets(string queueName, IEnumerable<TwitterStatus> tweets)
        {
            if (!MessageQueue.Exists(queueName))
            {
                Console.WriteLine("Queue {0} does not exist.", queueName);
                return;
            }

            string label = string.Format("TWEET@{0}", DateTime.Now.ToString("s"));
            try
            {
                MessageQueue queue = new MessageQueue(queueName);
                if (queue.Transactional)
                {
                    queue.Formatter = new System.Messaging.XmlMessageFormatter(new String[] { "System.String,mscorlib" });
                    
                    foreach (var tweet in tweets)
                    {
                        var content = BuildContent(tweet);                        
                        content = AddInfos(tweet, content);

                        Console.WriteLine("Sending \"{0}\" ...", label);
                        using (var ts = new TransactionScope())
                        {
                            queue.Send(content, label, MessageQueueTransactionType.Automatic);
                            ts.Complete();
                        }
                    }
                }
                else
                {
                    Console.WriteLine("Queue {0} is not transactional.", queueName);
                }
            }
            catch (Exception exc)
            {
                Console.Error.WriteLine(exc.Message);
            }
        }

        private static string BuildContent(TwitterStatus tweet)
        {
            string cd = tweet.CreatedDate.ToString(System.Globalization.DateTimeFormatInfo.CurrentInfo);
            string txt = tweet.Text.Replace('"', ' ');
            string content = String.Format("\"created_at\":\"{0}\",\"id\":{1},\"text\":\"{2}\",\"user\":\"{3}\"",
                                           cd, tweet.Id, txt, tweet.User.Name);
            return content;
        }

        private static string AddInfos(TwitterStatus tweet, string content)
        {            
            List<string> hashtags;
            List<string> mentions;
            List<string> urls;
            GetEntityInfos(tweet, out hashtags, out mentions, out urls);

            string ht = string.Join(",", hashtags);
            string mnt = string.Join(",", mentions);
            string url = string.Join(",", urls);
            content += String.Format(",\"hashtag\":[{0}],\"mention\":[{1}],\"url\":[{2}]",
                                     ht, mnt, url);
            return content;
        }

        private static void GetEntityInfos(TwitterStatus tweet, 
            out List<string> hashtagText, out List<string> mentionText, out List<string> urlText)
        {
            hashtagText = new List<string>();
            mentionText = new List<string>();
            urlText = new List<string>();

            string text = tweet.Text;
            if (string.IsNullOrEmpty(text))
                return;

            var coalescedEntities = tweet.Entities.Coalesce();
            foreach (var entity in coalescedEntities)
            {
                switch (entity.EntityType)
                {
                    case TwitterEntityType.HashTag:
                        var hashtag = ((TwitterHashTag)entity).Text;
                        Console.WriteLine("HashTag: " + hashtag);
                        hashtagText.Add("\"" + text.Substring(entity.StartIndex, entity.EndIndex - entity.StartIndex) + "\"");
                        break;
                    case TwitterEntityType.Mention:
                        var mention = ((TwitterMention)entity).ScreenName;
                        Console.WriteLine("Mention: " + mention);
                        mentionText.Add("\"" + text.Substring(entity.StartIndex, entity.EndIndex - entity.StartIndex) + "\"");
                        break;
                    case TwitterEntityType.Url:
                        var url = ((TwitterUrl)entity).Value;
                        Console.WriteLine("URL: " + url);
                        urlText.Add("\"" + text.Substring(entity.StartIndex, entity.EndIndex - entity.StartIndex) + "\"");
                        break;                    
                }
            }
        }

        private static IEnumerable<TwitterStatus> Get_tweets_on_specified_user_timeline(string screenName, long? lastId)
        {
            try
            {
                var serviceHelper = new TwitterServiceHelper();
                var service = serviceHelper.GetAuthenticatedService();

                return service.ListTweetsOnUserTimeline(new ListTweetsOnUserTimelineOptions { ScreenName = screenName, SinceId = lastId });
            }
            catch(Exception exc)
            {
                Console.WriteLine("Caught  exception: {0} - {1}", exc.Source, exc.Message);
                return null;
            }
        }

        private static IEnumerable<TwitterStatus> Get_tweets_from_search(string query, long? maxId)
        {
            IEnumerable<TwitterStatus> retValue = null;            

            try
            {
                var serviceHelper = new TwitterServiceHelper();
                var service = serviceHelper.GetAuthenticatedService();
                var italyGeoCode = new TwitterGeoLocationSearch(41.9, 12.5, 10, TwitterGeoLocationSearch.RadiusType.Mi);
                var results = service.Search(
                    new SearchOptions
                        {
                            Q = query, Geocode = italyGeoCode, Lang = "it"
                        });
                if (results != null)
                    retValue = results.Statuses;
            }
            catch (Exception exc)
            {
                Console.WriteLine("Caught  exception: {0} - {1}", exc.Source, exc.Message);
                return null;
            }
            return retValue;       
        }

        private static string[] Shuffle(string[] array)
        {
            var rnd = new Random(DateTime.Now.Millisecond);
            int n = array.Count();
            while (n > 0) 
            {
                var i = rnd.Next(0, n--); // 0 ≤ i < n
                var t = array[n];
                array[n] = array[i];
                array[i] = t;
            }
            return array;
        }

    }

    class TwitterServiceHelper
    {
        private readonly string _hero;
        private readonly string _consumerKey;
        private readonly string _consumerSecret;
        private readonly string _accessToken;
        private readonly string _accessTokenSecret;

        public TwitterServiceHelper()
        {
            _hero = ConfigurationManager.AppSettings["Hero"];
            _consumerKey = ConfigurationManager.AppSettings["ConsumerKey"];
            _consumerSecret = ConfigurationManager.AppSettings["ConsumerSecret"];
            _accessToken = ConfigurationManager.AppSettings["AccessToken"];
            _accessTokenSecret = ConfigurationManager.AppSettings["AccessTokenSecret"];
        }

        public TwitterService GetAuthenticatedService()
        {
            var service = new TwitterService(_consumerKey, _consumerSecret);
            service.TraceEnabled = true;
            service.AuthenticateWith(_accessToken, _accessTokenSecret);
            return service;
        }
    }
}
