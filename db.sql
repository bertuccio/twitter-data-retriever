-- Table: User
CREATE TABLE Users (
	id BIGINT PRIMARY KEY,
	created_at TIMESTAMP  NOT NULL,
	name TEXT NOT NULL,
	screen_name TEXT NOT NULL,
	lang TEXT NOT NULL,
	followers_count INTEGER DEFAULT 0)


-- Table: Tweet
CREATE TABLE Tweets (
	id BIGINT PRIMARY KEY,
	text TEXT NOT NULL, 
	created_at TIMESTAMP NOT NULL, 
	geo_lat DOUBLE DEFAULT NULL,
	geo_lng DOUBLE DEFAULT NULL, 
	country TEXT DEFAULT NULL,
	favorite_count INTEGER DEFAULT 0,
	in_reply_to_status_id BIGINT DEFAULT -1,
	in_reply_to_user_id BIGINT DEFAULT -1,
	quoted_status_id BIGINT DEFAULT -1,
	quoted_user_id BIGINT DEFAULT -1,
	retweet_count INTEGER DEFAULT 0,
	retweet_id BIGINT DEFAULT -1,
	user_id BIGINT REFERENCES Users (id))


CREATE TABLE IF NOT EXIST Hashtags(
	tweet_id BIGINT REFERENCE Users(id),
	hashtag TEXT NOT NULL)

