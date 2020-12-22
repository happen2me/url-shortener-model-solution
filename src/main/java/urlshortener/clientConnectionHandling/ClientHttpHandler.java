package urlshortener.clientConnectionHandling;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import urlshortener.AliasGenerationService;
import urlshortener.Database;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class ClientHttpHandler implements HttpHandler {

  private static AtomicLong requestID = new AtomicLong(0);

  private final Logger reference_logger = LogManager.getLogger("reference_log");
  private final Logger logger = LogManager.getLogger(this.getClass().getName());
  private Database database;
  private AliasGenerationService aliasGenerationService;

  public ClientHttpHandler(Database database, AliasGenerationService aliasGenerationService) {
    this.database = database;
    this.aliasGenerationService = aliasGenerationService;
  }

  public void handle(HttpExchange httpExchange) throws IOException {
    if ("GET".equals(httpExchange.getRequestMethod())) {
      handleGET(httpExchange);
    } else if ("POST".equals(httpExchange.getRequestMethod())) {
      handlePOST(httpExchange);
    } else {
      throw new UnsupportedOperationException("Cannot handle requests other than GET and POST.");
    }
  }

  private void handleGET(HttpExchange httpExchange) throws IOException {
    long id = requestID.incrementAndGet();

    String alias = httpExchange.getRequestURI().getPath();
    if (alias == null) {
      logger.error("GET requests should have an alias.");
      httpExchange.sendResponseHeaders(400, 0);
      httpExchange.close();
      return;
    }
    alias = alias.substring(1);  // Removes the leading slash
    reference_logger.info(String.format("RECEIVED_CLIENT_REQUEST(%d,GET,%s)", id, alias));

    try {
      String url = database.getUrl(alias);

      httpExchange.sendResponseHeaders(200, url.getBytes().length);
      OutputStream out = httpExchange.getResponseBody();
      out.write(url.getBytes());
      out.close();
      reference_logger.info(String.format("SEND_CLIENT_REPONSE(%d,GET,%s)", id, url));
    } catch (Database.NotExisting notExisting) {
      httpExchange.sendResponseHeaders(404, 0);
      httpExchange.close();
      reference_logger.info(String.format("SEND_CLIENT_REPONSE(%d,GET,%s)", id, "Not existing"));
    }
  }

  private void handlePOST(HttpExchange httpExchange) throws IOException {
    long id = requestID.incrementAndGet();

    List<String> body = new BufferedReader(
        new InputStreamReader(httpExchange.getRequestBody(), StandardCharsets.UTF_8))
        .lines()
        .collect(Collectors.toList());

    if (body.size() != 1) {
      logger.error("Invalid body for POST request, closing connection.");
      httpExchange.sendResponseHeaders(400, 0);
      httpExchange.close();
      return;
    }
    String url = body.get(0);

    reference_logger.info(String.format("RECEIVED_CLIENT_REQUEST(%d,POST,%s)", id, url));

    String alias = aliasGenerationService.addURL(url);
    httpExchange.sendResponseHeaders(200, alias.getBytes().length);
    OutputStream out = httpExchange.getResponseBody();
    out.write(alias.getBytes());
    out.close();

    reference_logger.info(String.format("SEND_CLIENT_REPONSE(%d,POST,%s)", id, alias));
  }


}
