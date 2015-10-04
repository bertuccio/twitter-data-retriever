package search;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.sql.SQLException;

import twitter4j.*;

import java.util.Date;
import java.util.List;
import java.util.Map;

import database.ConnectionManager;


/**
 * @author Yusuke Yamamoto - yusuke at mac.com
 * @since Twitter4J 2.1.7
 */
public class SearchTweets {
	/**
	 * Usage: java twitter4j.examples.search.SearchTweets [query]
	 *
	 * @param args
	 *            search query
	 * @throws InterruptedException
	 */

	public static void main(String[] args) throws InterruptedException {
		if (args.length < 1) {
			System.out.println("java twitter4j.examples.search.SearchTweets [query]");
			System.exit(-1);
		}
		
		
		ConnectionManager c = new ConnectionManager();
		try {
			c.createConnection();
			c.createDB();
			c.closeConnection();

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("conexion creada");

		boolean flag = false;
		do {
			while (flag);
			flag = true;
			long sleeptime = 0;
			Twitter twitter = new TwitterFactory().getInstance();
			try {
				Query query = new Query(args[0]);

				Map<String, RateLimitStatus> rateLimitStatus = twitter.getRateLimitStatus();
				for (String endpoint : rateLimitStatus.keySet()) {
					RateLimitStatus status = rateLimitStatus.get(endpoint);
					System.out.println("Endpoint: " + endpoint);
					System.out.println(" Limit: " + status.getLimit());
					System.out.println(" Remaining: " + status.getRemaining());
					System.out.println(" ResetTimeInSeconds: " + status.getResetTimeInSeconds());
					System.out.println(" SecondsUntilReset: " + status.getSecondsUntilReset());
				}

				QueryResult result;
				do {
					result = twitter.search(query);

					List<Status> tweets = result.getTweets();
					for (Status tweet : tweets) {
						// System.out.println("Remaining :
						// "+tweet.getRateLimitStatus().getRemaining());
						// System.out.println("Limit :
						// "+tweet.getRateLimitStatus().getLimit());
						// System.out.println("ResetTimeInSeconds :
						// "+tweet.getRateLimitStatus().getResetTimeInSeconds());
						// System.out.println("SecondsUntilReset :
						// "+tweet.getRateLimitStatus().getSecondsUntilReset());
						
						
						User u = tweet.getUser();
						try {
							System.out.println("User: id: " + u.getId() + " name: " + u.getName());
							c.insertTwitterUser(u);
						} catch (SQLException e1) {
							// TODO Auto-generated catch block
							System.out.println(e1);
						}
						
						if (tweet.isRetweet()) {
							
							System.out.println("RT Found!, getting the original tweet");
							Status retweetedStat = tweet.getRetweetedStatus();
							u = retweetedStat.getUser();
							System.out.println("RT User: id: " + u.getId() + " name: " + u.getName());

							try {
						
									c.insertTwitterUser(u);
									
									c.insertTweet(retweetedStat);
									
								
								
								
							} catch (SQLException e) {
								// TODO Auto-generated catch block
								System.out.println(e);
							}
							
						}
						
						try {
							c.insertTweet(tweet);
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					

					}
				} while ((query = result.nextQuery()) != null);
			} catch (TwitterException te) {

				// System.out.println(
				// te.getRateLimitStatus().getSecondsUntilReset());
				sleeptime = te.getRateLimitStatus().getSecondsUntilReset();
				// try {
				// Thread.sleep(100*1000);
				// } catch (InterruptedException e) {
				// TODO Auto-generated catch block
				// System.out.println(e.toString());
				// }
				System.out.println("Failed to search tweets: " + te.getMessage() + " SleepTime: " + sleeptime);

				Thread.sleep((sleeptime + 60) * 1000);
				flag = false;

				// Log.error(e.getLocalizedMessage());

			}
		} while (true);
	}
}