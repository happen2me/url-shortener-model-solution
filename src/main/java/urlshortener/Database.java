package urlshortener;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;


public class Database {

  public class NotExisting extends Exception {

  }

  private static final Logger referenceLogger = LogManager.getLogger("reference_log");
  private final static Logger logger = LogManager.getLogger(Database.class.getName());

  private Connection db = null;

  private final static String insertSQL = "INSERT INTO aliases(alias,url,expires) VALUES(?, ?, ?)";
  private PreparedStatement insertQuery;

  private final static String readSQL = "SELECT alias, url, expires FROM aliases WHERE alias=?";
  private PreparedStatement readQuery;

  public Database(String filePath, String datasetPath, int aliasLength) {
    connect(filePath);
    setup(aliasLength);
//    bulkloadDataset(datasetPath);
  }

  private void bulkloadDataset(String datasetPath) {
    try {
      int aliasCount = db.createStatement().executeQuery("SELECT COUNT(*) FROM aliases").getInt(1);
      if (aliasCount == 0) {
        logger.info("Bulkloading dataset");
        // TODO
        logger.info("Dataset loaded");
      }
    } catch (SQLException e) {
      logger.error(e.getMessage());
      throw new RuntimeException("Could not load CSV file.");
    }
  }

  private void connect(String filePath) {
    String databasePath = "jdbc:sqlite:" + filePath;
    try {
      db = DriverManager.getConnection(databasePath);
      logger.info("Established connection to database: " + databasePath);
    } catch (SQLException e) {
      logger.error(e.getMessage());
      throw new RuntimeException("Could not connect to DB");
    }
  }

  synchronized private void setup(int aliasLength) {
    String createTableSQL = String.format("CREATE TABLE IF NOT EXISTS aliases (\n" + "	alias char(%s) PRIMARY KEY,\n" + "	url varchar(500) NOT NULL,\n" + "	expires timestamp\n" + ");", aliasLength);

    String createIndexExpiresSQL = "CREATE INDEX IF NOT EXISTS expiring_index ON aliases (expires)";

    try {
      Statement statement = db.createStatement();
      statement.execute(createTableSQL);
      statement.execute(createIndexExpiresSQL);
    } catch (SQLException e) {
      logger.error(e.getMessage());
      throw new RuntimeException("Could not create table for URLs");
    }

    try {
      insertQuery = db.prepareStatement(insertSQL);
      readQuery = db.prepareStatement(readSQL);
    } catch (SQLException e) {
      logger.error(e.getMessage());
      throw new RuntimeException("Could not prepare statements.");
    }
  }


  synchronized public String getUrl(String alias) throws NotExisting {
    try {
      readQuery.setString(1, alias);
    } catch (SQLException e) {
      logger.error(e.getMessage());
      throw new RuntimeException("Could not prepare read query.");
    }

    try {
      ResultSet result = readQuery.executeQuery();

      if (!result.next()) {
        throw new NotExisting();
      }

      referenceLogger.info(String.format("READ_STORAGE_SUCCESS(%s)", alias));
      return result.getString("url");
    } catch (SQLException e) {
      logger.error(e.getMessage());
      throw new RuntimeException("Unknown exception.");
    }
  }

  synchronized public boolean addURL(String alias, String url, Timestamp timestamp) {
    try {
      insertQuery.setString(1, alias);
      insertQuery.setString(2, url);
      insertQuery.setTimestamp(3, timestamp);
    } catch (SQLException e) {
      logger.error(e.getMessage());
      throw new RuntimeException("Problem setting up insert query");
    }

    try {
      int updatedRecords = insertQuery.executeUpdate();
      if (updatedRecords != 1) {
        logger.error("No record was updated.");
        return false;
      }
      return true;
    } catch (SQLException e) {
      logger.debug(e.getMessage());
      return false;
    }
  }
}
