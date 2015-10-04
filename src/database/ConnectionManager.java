package database;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.ResourceBundle;


import twitter4j.HashtagEntity;
import twitter4j.Status;
import twitter4j.User;

public class ConnectionManager {
	


	private final static String driverName;
	private final static String connectionUrl;
	private final static String PROPERTY_FILE = "dbconfig";
	private final static String PROPERTY_FIELD_DBDRIVER = "dbdriver";
	private final static String PROPERTY_FIELD_DATABASE = "database";

	private Connection con = null;

	static {

		ResourceBundle bundle = ResourceBundle.getBundle(PROPERTY_FILE);
		driverName = bundle.getString(PROPERTY_FIELD_DBDRIVER);
		String sJdbc = "jdbc:sqlite:";

		String sTempDb = bundle.getString(PROPERTY_FIELD_DATABASE) + ".db";
		connectionUrl = sJdbc + sTempDb;

		try {
			Class.forName(driverName);

		} catch (ClassNotFoundException e) {
			System.out.println(e.toString());
		}

	}
	
	public Connection createConnection() throws SQLException {

		con = DriverManager.getConnection(connectionUrl);

		return con;
	}

	public void closeConnection() throws SQLException {

		this.con.close();
	}

	public void createDB() throws SQLException{

		Statement stmt = null;

		stmt = con.createStatement();
		
		
        String sql = "CREATE TABLE IF NOT EXISTS Users " +
                     "(id BIGINT PRIMARY KEY," +
                     " created_at TIMESTAMP NOT NULL, " + 
                     " name TEXT NOT NULL, " + 
                     " screen_name TEXT NOT NULL, " + 
                     " lang TEXT NOT NULL, " + 
                     " verified BOOLEAN NOT NULL," +
                     " followers_count INTEGER DEFAULT 0)"; 
        
        stmt.executeUpdate(sql);
    	stmt.close();
        
        sql = "CREATE TABLE IF NOT EXISTS Tweets " +
                "(id BIGINT PRIMARY KEY," +
                " text TEXT NOT NULL,  " + 
                " created_at TIMESTAMP NOT NULL, " + 
                " geo_lat DOUBLE DEFAULT NULL, " + 
                " geo_lng DOUBLE DEFAULT NULL, " + 
                " country TEXT DEFAULT NULL, " + 
                " favorite_count INTEGER DEFAULT 0, " + 
                " in_reply_to_status_id BIGINT DEFAULT -1, " + 
                " in_reply_to_user_id BIGINT DEFAULT -1, " + 
                " quoted_status_id BIGINT DEFAULT -1, " + 
                " quoted_user_id BIGINT DEFAULT -1, " + 
                " retweet_count INTEGER DEFAULT 0, " + 
                " retweet_id BIGINT DEFAULT -1, " + 
                " user_id BIGINT REFERENCES Users (id))"; 

		stmt.executeUpdate(sql);
		stmt.close();
		
		
		sql = "CREATE TABLE IF NOT EXISTS Hashtags " +
	                "(tweet_id BIGINT REFERENCES Users (id)," +
	                " hashtag NOT NULL)"; 

		
		stmt.executeUpdate(sql);
    	stmt.close();
    	
		  sql = "CREATE TABLE IF NOT EXISTS Followings " +
				  "(user_id BIGINT REFERENCES Users (id)," +
				  " following_id BIGINT REFERENCES Users (id))"; 
		
			
		stmt.executeUpdate(sql);
		stmt.close();
        
      
	}
	
	public void insertTwitterUser(User u) throws SQLException{
		
		if(!existsTwitterUser(u.getId())){
			insertTwitterUser(u.getId(), u.getCreatedAt(), u.getName(), u.getScreenName(),
					u.getLang(), u.isVerified(), u.getFollowersCount());
		}
		
	}

	private void insertTwitterUser(Long id, Date createdAt, String name, String screenName, String lang,
			boolean verified, int followersCount) throws SQLException {

		PreparedStatement stmt = null;
		
		//java.sql.Date sqlDate = new java.sql.Date(createdAt.getTime());
		Timestamp timeStamp = new Timestamp(createdAt.getTime());
		
		this.createConnection();
		stmt = con.prepareStatement("INSERT INTO Users (id, created_at, name, screen_name, lang, verified, followers_count) "
				+ "values (?, ?, ?, ?, ?, ?, ?)");
		stmt.setLong(1, id);
		stmt.setTimestamp(2, timeStamp);
		//stmt.setDate(2,  sqlDate);
		stmt.setString(3, name);
		stmt.setString(4, screenName);
		stmt.setString(5, lang);
		stmt.setBoolean(6, verified);
		stmt.setInt(7, followersCount);
		stmt.executeUpdate();
		
		stmt.close();
		this.closeConnection();
	}
	
	public boolean existsTwitterUser(Long id) throws SQLException{
		
		PreparedStatement stmt = null;
		boolean exists;
		
		this.createConnection();
		stmt = con.prepareStatement("SELECT id FROM USERS WHERE id=?");
		stmt.setLong(1, id);
		exists = stmt.executeQuery().next();
		stmt.close();
		this.closeConnection();

		return exists;
	}
	
	public void insertTweet(Status tweet) throws SQLException {

		Double geo_lat = null;
		Double geo_lng = null;
		String country = null;
		Long quoted_status_id = new Long(-1);
		Long quoted_user_id = new Long(-1);
		Long retweet_id = new Long(-1);
		
		insertTwitterUser(tweet.getUser());
		
		if(!existsTweet(tweet.getId())){
			
	
			if (tweet.getGeoLocation() != null) {
				geo_lat = tweet.getGeoLocation().getLatitude();
				geo_lng = tweet.getGeoLocation().getLongitude();
			}
			if (tweet.getPlace() != null)
				country = tweet.getPlace().getCountry();
	
			if (tweet.getQuotedStatus() != null) {
				quoted_status_id = tweet.getQuotedStatus().getId();
				if(tweet.getQuotedStatus().getUser() != null)
					quoted_user_id = tweet.getQuotedStatus().getUser().getId();
			}
	
			if (tweet.isRetweet())
				retweet_id = tweet.getRetweetedStatus().getId();
	
			insertTweet(tweet.getId(), tweet.getText(), tweet.getCreatedAt(), geo_lat, geo_lng, country,
					tweet.getFavoriteCount(), tweet.getInReplyToStatusId(), tweet.getInReplyToUserId(), quoted_status_id,
					quoted_user_id, tweet.getRetweetCount(), retweet_id, tweet.getUser().getId());
			
			insertHashtag(tweet);
			
		}

	}
	
	
	public void insertHashtag(Status tweet) throws SQLException {
		

		for(HashtagEntity h : tweet.getHashtagEntities()){
			insertHashtag(tweet.getId(), h.getText());
		}
		
		
	}
	
	public void insertHashtag(Long tweet_id, String hashtag) throws SQLException{
		
		PreparedStatement stmt = null;
				
		this.createConnection();
		stmt = con.prepareStatement("INSERT INTO Hashtags (tweet_id, hashtag) "
				+ "values (?, ?)");
		stmt.setLong(1, tweet_id);
		stmt.setString(2, hashtag);

		stmt.executeUpdate();
		stmt.close();
		this.closeConnection();
		
	}
	
	public void inserFollowing(Long user_id, Long following_id) throws SQLException{
		
		PreparedStatement stmt = null;
				
		this.createConnection();
		stmt = con.prepareStatement("INSERT INTO Followings (user_id, following_id) "
				+ "values (?, ?)");
		stmt.setLong(1, user_id);
		stmt.setLong(2, following_id);

		stmt.executeUpdate();
		stmt.close();
		this.closeConnection();
		
	}
	
	
	private void insertTweet(Long id, String text, Date created_at, Double geo_lat, Double geo_lng, String country,
			int favorite_count, Long in_reply_to_status_id, Long in_reply_to_user_id, Long quoted_status_id,
			Long quoted_user_id, int retweet_count, Long retweet_id, Long user_id) throws SQLException {

		PreparedStatement stmt = null;

		// java.sql.Date sqlDate = new java.sql.Date(createdAt.getTime());
		Timestamp timeStamp = new Timestamp(created_at.getTime());

		this.createConnection();
		String queryString = "INSERT INTO TWEETS (id, text, created_at, favorite_count, in_reply_to_status_id, "
				+ "in_reply_to_user_id, quoted_status_id, quoted_user_id, retweet_count, retweet_id, user_id";
		String queryValues = " values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?";

		if (geo_lat != null && geo_lng != null) {
			queryString += ", geo_lat, geo_lng";
			queryValues += ", ?, ?";

		}
		if (country != null) {
			queryString += ", country";
			queryValues += ", ?";
		}

		queryString += ")";
		queryValues += ")";

		stmt = con.prepareStatement(queryString + queryValues);
		stmt.setLong(1, id);
		stmt.setString(2, text);
		stmt.setTimestamp(3, timeStamp);
		stmt.setInt(4, favorite_count);
		stmt.setLong(5, in_reply_to_status_id);
		stmt.setLong(6, in_reply_to_user_id);
		stmt.setLong(7, quoted_status_id);
		stmt.setLong(8, quoted_user_id);
		stmt.setInt(9, retweet_count);
		stmt.setLong(10, retweet_id);
		stmt.setLong(11, user_id);
		if (country != null && geo_lat != null && geo_lng != null) {
			stmt.setDouble(12, geo_lat);
			stmt.setDouble(13, geo_lng);
			stmt.setString(14, country);
		} else if (country != null) {
			stmt.setString(12, country);
		}

		stmt.executeUpdate();
		stmt.close();
		this.closeConnection();
	}

	private boolean existsTweet(Long id) throws SQLException{
		
		PreparedStatement stmt = null;
		boolean exists;
		
		this.createConnection();
		stmt = con.prepareStatement("SELECT id FROM TWEETS WHERE id=?");
		stmt.setLong(1, id);
		exists = stmt.executeQuery().next();
		stmt.close();
		this.closeConnection();

		return exists;
	}
	
	public void recursiveInsert(Status status, int profundidad) throws SQLException {
		
		System.out.println("Profundidad: "+profundidad);
		if(status.isRetweet())
			recursiveInsert(status.getRetweetedStatus(), profundidad + 1);
		if(status.getQuotedStatus() != null)
			recursiveInsert(status.getQuotedStatus(), profundidad + 1);
		
		insertTweet(status);
		
		
		
	}
	
	public ArrayList<Long> getUsersById() throws SQLException{
		
		PreparedStatement stmt = null;
		ArrayList<Long> result = null;
		this.createConnection();
		stmt = con.prepareStatement("SELECT id FROM Users ORDER BY id ASC");
		ResultSet  set = stmt.executeQuery();
		
		
		if(set.next()){
			
			result = new ArrayList<Long>();
			do{

			    Long id = set.getLong("id");
			    result.add(id);
			    
			}while(set.next());
			System.out.println("Numerooo " + result.size());
		}
		
		stmt.close();
		this.closeConnection();
		
		return result;
	}
	



			
	
	



}