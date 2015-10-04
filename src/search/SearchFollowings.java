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
public class SearchFollowings {
	/**
	 * Usage: java twitter4j.examples.search.SearchTweets [query]
	 *
	 * @param args
	 *            search query
	 * @throws InterruptedException
	 */

	public static void main(String[] args) throws InterruptedException {
		
		
		Long lastUser = Long.parseLong(args[0]);
		
		do{

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
			
			Twitter twitter = new TwitterFactory().getInstance();
	
			long lCursor = -1;
			IDs friendsIDs = null;
			try {
				for(Long user_id : c.getUsersById()){
					
					if(user_id >= lastUser){
					
						System.out.println("ID usuario consultado" + user_id);
						
						boolean flag;
						PagableResponseList<User> lists = null;
								
						do
						{
							do{
								try {
									lists = twitter.getFriendsList(user_id, lCursor);
									//friendsIDs = twitter.getFriendsIDs(user_id, lCursor);
									flag = false;
								} catch (TwitterException e) {
									System.out.println("ID usuario consultado al saltar excepcion: " + user_id);
									// TODO Auto-generated catch block
									flag = true;
									e.printStackTrace();
									System.exit(-1);
								}
							}while(flag);
					
							
							int contador = 0;
							for (User following : lists) {
								 if(c.existsTwitterUser(following.getId())){
							    	   contador++;
							    	   System.out.println("Total: "+lists.size()+ " Contadorr"+contador);
							    	   c.inserFollowing(user_id, following.getId());
							       }
							}
//							 for (long following : friendsIDs.getIDs())
//							   {
//							       if(c.existsTwitterUser(following)){
//							    	   contador++;
//							    	   System.out.println("Total: "+friendsIDs.getIDs().length+ " Contadorr"+contador);
//							    	   c.inserFollowing(user_id, following);
//							       }
//							   }
						//} while ((lCursor = friendsIDs.getNextCursor()) != 0);
					} while ((lCursor = lists.getNextCursor()) != 0);	
					
					}	
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
		}while(true);

		
	}
}