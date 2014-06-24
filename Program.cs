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

            string topicName = "mondiali2014";
            var tweets = Get_tweets_from_search(topicName);
            if (tweets != null)
            {
                if (tweets.Any())
                {
                    Console.WriteLine(string.Format("{0} - Getting tweets by search #{1}...",
                                                    DateTime.Now.ToString("s"), topicName));

                    foreach (var tweetsByUser in tweets.GroupBy(t => t.User.Id))
                    {
                        var lastTweet = tweetsByUser.OrderBy(t => t.Id).Last();
                        var userName = lastTweet.User.ScreenName;                        
                                        
                        Console.WriteLine(string.Format("{0} - Sending tweets from @{1} to {2}...", 
                            DateTime.Now.ToString("s"), userName, queueName));
                        SendTweets(queueName, tweetsByUser);
                    }                    
                }
            }

            //GetTweetsByUsers(lastIdByUser, isWritten2File, pathFile, isFirstUser, queueName);
        }

        private static void GetTweetsByUsers(Dictionary<string, long?> lastIdByUser, bool isWritten2File, string pathFile, bool isFirstUser,
                                             string queueName)
        {
            long? lastId;
            ConfigurationManager.RefreshSection("AppSettings");
            string userNames = ConfigurationManager.AppSettings["UserNames"];
            if (!string.IsNullOrEmpty(userNames))
            {
                foreach (string userName in userNames.Split(new char[] {' '}))
                {
                    if (!lastIdByUser.TryGetValue(userName, out lastId))
                        lastId = null;

                    var tweets = Get_tweets_on_specified_user_timeline(userName, lastId);
                    if (tweets != null)
                    {
                        if (tweets.Any())
                        {
                            Console.WriteLine(string.Format("{0} - Getting tweets from @{1}...",
                                                            DateTime.Now.ToString("s"), userName));
                            lastId = tweets.OrderBy(t => t.Id).Last().Id;
                            lastIdByUser[userName] = lastId.Value;
                            if (isWritten2File)
                            {
                                WriteTweets(pathFile, isFirstUser, tweets);
                                isFirstUser = false;
                            }
                            else
                            {
                                Console.WriteLine(string.Format("{0} - Sending tweets from @{1} to {2}...",
                                                                DateTime.Now.ToString("s"), userName, queueName));
                                SendTweets(queueName, tweets);
                            }
                        }
                    }
                }
            }
            else
            {
                Console.Error.WriteLine("Missing appSetting in the config: UserNames");
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
                        //GetEntityInfos(tweet);

                        string cd = tweet.CreatedDate.ToString(System.Globalization.DateTimeFormatInfo.CurrentInfo);
                        string txt = tweet.Text.Replace('"', ' ');
                        string content = String.Format("\"created_at\":\"{0}\", \"id\":{1}, \"text\":\"{2}\", \"user\":\"{3}\"",
                            cd, tweet.Id, txt, tweet.User.Name);
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

        private static void GetEntityInfos(TwitterStatus tweet, 
            out List<string> hashtagText, List<string> mentionText, List<string> urlText)
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
                    default:
                        throw new ArgumentOutOfRangeException();
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

        private static IEnumerable<TwitterStatus> Get_friend_tweets(string screenName)
        {
            var tweets = new List<TwitterStatus>();
            try
            {
                var serviceHelper = new TwitterServiceHelper();
                var service = serviceHelper.GetAuthenticatedService();
                var friendIds = service.ListFriendIdsOf(new ListFriendIdsOfOptions() { ScreenName = screenName, Count = 100 });
                foreach (var id in friendIds)
                {
                    var tweet = service.GetTweet(new GetTweetOptions { Id = id });
                    if (tweet != null)
                        tweets.Add(tweet);
                }
            }
            catch (Exception exc)
            {
                Console.Error.WriteLine(exc.Message);
            }
            return tweets;
        }

        private static IEnumerable<TwitterStatus> Get_tweets_on_specified_topic(string topicName)
        {
            IEnumerable<TwitterStatus> retValue = null;
            try
            {
                var serviceHelper = new TwitterServiceHelper();
                var service = serviceHelper.GetAuthenticatedService();

                var options = new ListSuggestedUsersOptions();
                options.Lang = "it";
                options.Slug = topicName;
                var twitterUsers = service.ListSuggestedUsers(options);
                
            }
            catch (Exception exc)
            {
                Console.WriteLine("Caught  exception: {0} - {1}", exc.Source, exc.Message);
                return null;
            }
            return retValue;
        }

        private static IEnumerable<TwitterStatus> Get_tweets_from_search(string topicName)
        {
            IEnumerable<TwitterStatus> retValue = null;
            try
            {
                var serviceHelper = new TwitterServiceHelper();
                var service = serviceHelper.GetAuthenticatedService();
                var results = service.Search(new SearchOptions { Q = topicName });
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
