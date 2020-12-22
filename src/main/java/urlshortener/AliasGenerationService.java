package urlshortener;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Timestamp;
import java.util.Base64;

public class AliasGenerationService {
  private static final Logger referenceLogger = LogManager.getLogger("reference_log");
  private static final Logger logger = LogManager.getLogger(AliasGenerationService.class.getName());

  private static final int EXPIRES_AFTER = 1000 * 60 * 60 * 24 * 365 * 5;

  private int aliasLength;
  private Database database;
  private MessageDigest md5;

  AliasGenerationService(Database database, int aliasLength) {
    this.database = database;
    this.aliasLength = aliasLength;

    try {
      md5 = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("Needs MD5 to be installed.");
    }
  }

  public String addURL(String url) {
    Timestamp expires = new Timestamp(System.currentTimeMillis() + EXPIRES_AFTER);

    String uniqueURL = url;
    String alias = generateAlias(uniqueURL);
    int unique = -1;
    while (!database.addURL(alias, url, expires)) {
      logger.debug(String.format("Collision on url %s with alias %s", url, alias));

      uniqueURL = url + unique;
      alias = generateAlias(uniqueURL);
      unique++;
    }

    referenceLogger.info(String.format("LOCAL_WRITE(%s)", alias));
    return alias;
  }

  private String generateAlias(String url) {
    byte [] hash = md5.digest(url.getBytes());
    String alias =   Base64.getEncoder().encodeToString(hash).substring(0, aliasLength);
    referenceLogger.info(String.format("GENERATED_HASH_FOR_URL(%s,%s)", alias, url));
    return alias;
  }
}
