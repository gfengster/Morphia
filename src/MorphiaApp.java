import com.abc.Address;
import com.abc.Person;
import com.mongodb.*;
import com.mongodb.MapReduceCommand.OutputType;
import com.mongodb.client.MongoIterable;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Key;
import org.mongodb.morphia.query.Query;
import org.mongodb.morphia.query.UpdateOperations;
import org.mongodb.morphia.query.UpdateResults;

import java.util.List;

public class MorphiaApp {

	private static ConnectDB connectDB;
	private Datastore datastore;

	public static void main(String[] args) {
		final MorphiaApp app = new MorphiaApp();
		app.clean();
		app.create();
		app.update();
		app.delete();
		app.list();
		app.mapreduce();
		connectDB.closeDb();
	}
	
	private MorphiaApp() {
		connectDB=new ConnectDB();
		datastore = ConnectDB.getDatastore();
	}
	
	private void clean() {

		final MongoIterable<String> dbs = connectDB.getDBNames();

		for (String db : dbs) {
			System.out.println(db);
		}
		
		connectDB.dropDB();
		
		connectDB.closeDb();
	}
	
	private void create() {

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

	}
	
	private void update() {

		final UpdateOperations<Person> updateOps = datastore.createUpdateOperations(Person.class)
							.set("lastName", "Smith").set("sex", "male");
		
		Query<Person> query = datastore.createQuery(Person.class).field("lastName").equal("Smith 9");
		final Person p = datastore.findAndModify(query, updateOps);
		System.out.println("Updated: " + p);

		query = datastore.createQuery(Person.class).field("lastName").equal("Smith 8");
		final UpdateResults ret = datastore.update(query, updateOps);
		System.out.println("Updated: " + ret.getUpdatedCount());

	}
	
	private void delete() {

		final Query<Person> query = datastore.createQuery(Person.class).field("lastName").equal("Smith 7");
		final WriteResult ret = datastore.delete(query);
		
		System.out.println("Removed: " + ret.getN());

	}
	
	private void list(){

		final Query<Person> query = datastore.createQuery(Person.class);
		final List<Person> persons = query.asList();
		
		for(Person p : persons) {
			System.out.println("Person: " + p);
		}

	}
	
	private void mapreduce(){

		final String map = "function(){emit(this.lastName, 1);}";
		final String reduce = "function(key, values){return Array.sum(values)},{query:{lastName:\"Smith\"}, out:\"person\"}";
		
		final DBCollection collection = datastore.getCollection(Person.class);
		
		final MapReduceCommand cmd = new MapReduceCommand(collection, map, reduce, null,
				OutputType.INLINE, null);
		
		final MapReduceOutput out = collection.mapReduce(cmd);
		
		for(DBObject o : out.results()){
			System.out.println(o.toString());  
		}

	}
}
