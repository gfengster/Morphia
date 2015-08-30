import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoIterable;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Morphia;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created on 8/30/15.
 *
 * @author @merzak7
 */
public class ConnectDB {

    private final String mDB;
    private final List<ServerAddress> mServers;
    private static Morphia morphia;
    private static Datastore datastore;
    private final MongoClient mongoClient;

    public ConnectDB() {
        Logger mongoLogger = Logger.getLogger("org.mongodb.driver");
        mongoLogger.setLevel(Level.SEVERE);
        mDB = "morphiaDB";
        mServers = new ArrayList<ServerAddress>(3);
        mServers.add(new ServerAddress("localhost", 28001));
        mServers.add(new ServerAddress("localhost", 28002));
        mServers.add(new ServerAddress("localhost", 28003));

        mongoClient = new MongoClient(mServers);
        morphia = new Morphia();
        morphia.mapPackage("com.abc");
        morphia.createDatastore(mongoClient, mDB);
    }

    public static Datastore getDatastore() {
        return datastore;
    }

    public MongoIterable<String> getDBNames() {
        return mongoClient.listDatabaseNames();
    }

    public void closeDb() {
        morphia = null;
        datastore = null;
        mongoClient.close();
    }

    public void dropDB() {
        mongoClient.dropDatabase(mDB);
    }
}
