import java.util.ArrayList;
import java.util.List;

import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Key;
import org.mongodb.morphia.Morphia;
import org.mongodb.morphia.query.Query;
import org.mongodb.morphia.query.UpdateOperations;
import org.mongodb.morphia.query.UpdateResults;

import com.abc.Address;
import com.abc.Person;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceCommand.OutputType;
import com.mongodb.MapReduceOutput;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.WriteResult;
import com.mongodb.client.MongoIterable;

public class MorphiaApp {

	public static void main(String[] args) {
		final MorphiaApp app = new MorphiaApp();
		app.clean();
		app.create();
		app.update();
		app.delete();
		app.list();
		app.mapreduce();
	}

	private final String mDB;
	private final List<ServerAddress> mServers;
	private final Morphia morphia;
	
	private MorphiaApp() {
		mDB = "morphiaDB";
		mServers = new ArrayList<ServerAddress>(3);
		mServers.add(new ServerAddress("localhost", 28001));
		mServers.add(new ServerAddress("localhost", 28002));
		mServers.add(new ServerAddress("localhost", 28003));
		
		morphia = new Morphia();
		morphia.mapPackage("com.abc");
	}
	
	private void clean() {
		final MongoClient mongoClient = new MongoClient(mServers);

		final MongoIterable<String> dbs = mongoClient.listDatabaseNames();

		for (String db : dbs) {
			System.out.println(db);
		}
		
		mongoClient.dropDatabase(mDB);		
		
		mongoClient.close();
	}
	
	private void create() {
		final MongoClient mongoClient = new MongoClient(mServers);

		final Datastore datastore = morphia.createDatastore(mongoClient, mDB);
		datastore.ensureIndexes();

		for (int i = 0; i < 10; i++) {
			final Address address1 = new Address();
			address1.setType("Home");
			address1.setCity("Edinburgh" + " " + i);
			address1.setState("Midlothian" + " " + i);
			address1.setStreet("Prices Street" + " " + i);
			address1.setZipCode("EH1" + " " + i);

			Key<Address> akey = datastore.save(address1);
			System.out.println("Created " + akey);
			
			final Address address2 = new Address();
			address2.setType("Work");
			address2.setCity("Edinburgh" + " " + i + "-1");
			address2.setState("Midlothian" + " " + i + "-1");
			address2.setStreet("Prices Street" + " " + i + "-1");
			address2.setZipCode("EH1" + " " + i + "-1");
			
			akey = datastore.save(address2);
			System.out.println("Created " + akey);
			
			final Person person = new Person();
			person.getAddress().add(address1);
			person.getAddress().add(address2);

			person.setFirstName("John" + " " + i);
			person.setLastName("Smith" + " " + i);
			
			final Key<Person> pkey = datastore.save(person);
			System.out.println("Created " + pkey);
		}

		mongoClient.close();
	}
	
	private void update() {
		final MongoClient mongoClient = new MongoClient(mServers);

		final Datastore datastore = morphia.createDatastore(mongoClient, mDB);
				
		final UpdateOperations<Person> updateOps = datastore.createUpdateOperations(Person.class)
							.set("lastName", "Smith").set("sex", "male");
		
		Query<Person> query = datastore.createQuery(Person.class).field("lastName").equal("Smith 9");
		final Person p = datastore.findAndModify(query, updateOps);
		System.out.println("Updated: " + p);

		query = datastore.createQuery(Person.class).field("lastName").equal("Smith 8");
		final UpdateResults ret = datastore.update(query, updateOps);
		System.out.println("Updated: " + ret.getUpdatedCount());
		
		mongoClient.close();
	}
	
	private void delete() {
		final MongoClient mongoClient = new MongoClient(mServers);

		final Datastore datastore = morphia.createDatastore(mongoClient, mDB);
		
		final Query<Person> query = datastore.createQuery(Person.class).field("lastName").equal("Smith 7");
		final WriteResult ret = datastore.delete(query);
		
		System.out.println("Removed: " + ret.getN());
		
		mongoClient.close();
	}
	
	private void list(){
		final MongoClient mongoClient = new MongoClient(mServers);

		final Datastore datastore = morphia.createDatastore(mongoClient, mDB);
		
		final Query<Person> query = datastore.createQuery(Person.class);
		final List<Person> persons = query.asList();
		
		for(Person p : persons) {
			System.out.println("Person: " + p);
		}
		
		mongoClient.close();
	}
	
	private void mapreduce(){
		final MongoClient mongoClient = new MongoClient(mServers);

		final Datastore datastore = morphia.createDatastore(mongoClient, mDB);
		
		final String map = "function(){emit(this.lastName, 1);}";
		final String reduce = "function(key, values){return Array.sum(values)},{query:{lastName:\"Smith\"}, out:\"person\"}";
		
		final DBCollection collection = datastore.getCollection(Person.class);
		
		final MapReduceCommand cmd = new MapReduceCommand(collection, map, reduce, null,
				OutputType.INLINE, null);
		
		final MapReduceOutput out = collection.mapReduce(cmd);
		
		for(DBObject o : out.results()){
			System.out.println(o.toString());  
		}
		
		mongoClient.close();
	}
}
