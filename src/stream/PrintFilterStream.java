package stream;

/*
 * Copyright 2007 Yusuke Yamamoto
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import twitter4j.*;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;

import database.ConnectionManager;

/**
 * <p>
 * This is a code example of Twitter4J Streaming API - filter method support.
 * <br>
 * Usage: java twitter4j.examples.stream.PrintFilterStream [follow(comma
 * separated numerical user ids)] [track(comma separated filter terms)]<br>
 * </p>
 *
 * @author Yusuke Yamamoto - yusuke at mac.com
 */
public final class PrintFilterStream {
	/**
	 * Main entry of this application.
	 *
	 * @param args
	 *            follow(comma separated user ids) track(comma separated filter
	 *            terms)
	 * @throws TwitterException
	 *             when Twitter service or network is unavailable
	 */
	public static int counter = 0;



	public static void main(String[] args) throws TwitterException, IOException {

		if (args.length < 1) {
			System.out.println(
					"Usage: java twitter4j.examples.PrintFilterStream [follow(comma separated numerical user ids)] [track(comma separated filter terms)]");
			System.exit(-1);
		}

		ConnectionManager c = new ConnectionManager();
		try {
			c.createConnection();
			c.createDB();
			c.closeConnection();

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.println(e);
		}
		System.out.println("conexion creada");

		StatusListener listener = new StatusListener() {
			@Override
			public void onStatus(Status status) {
				
//				pw.println("@" + status.getUser().getScreenName() + " - " + status.getText());
//				if (status.getGeoLocation() != null)
//					pw.println("Lat:" + status.getGeoLocation().getLatitude() + "Long"
//							+ status.getGeoLocation().getLongitude());
				
//				User u = status.getUser();
//				try {
//					System.out.println("User: id: " + u.getId() + " name: " + u.getName());
//					c.insertTwitterUser(u);
//				} catch (SQLException e1) {
//					// TODO Auto-generated catch block
//					System.out.println(e1);
//				}
//				
//				if (status.isRetweet()) {
//					
//					System.out.println("RT Found!, getting the original tweet");
//					Status retweetedStat = status.getRetweetedStatus();
//					u = retweetedStat.getUser();
//					System.out.println("RT User: id: " + u.getId() + " name: " + u.getName());
//
//					try {
//				
//							c.insertTwitterUser(u);
//							
//							c.insertTweet(retweetedStat);
//							
//						
//						
//						
//					} catch (SQLException e) {
//						// TODO Auto-generated catch block
//						System.out.println(e);
//					}
//					
//				}
//				if(status.getQuotedStatus() != null){
//					
//					System.out.println("Quoted Found!, getting the original tweet");
//					Status quotedStatus = status.getQuotedStatus();
//					u = quotedStatus.getUser();
//					System.out.println("Quoted User: id: " + u.getId() + " name: " + u.getName());
//
//					try {
//						c.insertTwitterUser(u);
//						c.insertTweet(quotedStatus);
//					} catch (SQLException e) {
//						// TODO Auto-generated catch block
//						System.out.println(e);
//					}
//				}
//				
//		
//				
//				try {
//					c.insertTweet(status);
//				} catch (SQLException e) {
//					// TODO Auto-generated catch block
//					System.out.println(e);
//				}
				
				try {
					c.recursiveInsert(status, 0);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
				System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
			}

			@Override
			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
				System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
			}

			@Override
			public void onScrubGeo(long userId, long upToStatusId) {
				System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
			}

			@Override
			public void onStallWarning(StallWarning warning) {
				System.out.println("Got stall warning:" + warning);
			}

			@Override
			public void onException(Exception ex) {
				ex.printStackTrace();
			}
		};

		TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
		twitterStream.addListener(listener);
		ArrayList<Long> follow = new ArrayList<Long>();
		ArrayList<String> track = new ArrayList<String>();
		for (String arg : args) {
			if (isNumericalArgument(arg)) {
				for (String id : arg.split(",")) {
					follow.add(Long.parseLong(id));
				}
			} else {
				track.addAll(Arrays.asList(arg.split(",")));
			}
		}
		long[] followArray = new long[follow.size()];
		for (int i = 0; i < follow.size(); i++) {
			followArray[i] = follow.get(i);
		}
		String[] trackArray = track.toArray(new String[track.size()]);

		// filter() method internally creates a thread which manipulates
		// TwitterStream and calls these adequate listener methods continuously.
		twitterStream.filter(new FilterQuery(0, followArray, trackArray));

	}

	private static boolean isNumericalArgument(String argument) {
		String args[] = argument.split(",");
		boolean isNumericalArgument = true;
		for (String arg : args) {
			try {
				Integer.parseInt(arg);
			} catch (NumberFormatException nfe) {
				isNumericalArgument = false;
				break;
			}
		}
		return isNumericalArgument;
	}
}