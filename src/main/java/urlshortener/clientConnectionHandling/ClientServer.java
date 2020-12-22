package urlshortener.clientConnectionHandling;

import com.sun.net.httpserver.HttpServer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import urlshortener.AliasGenerationService;
import urlshortener.Database;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ClientServer {
  private static final Logger logger = LogManager.getLogger(ClientServer.class.getName());

  private HttpServer httpServer;
  private ExecutorService executorService;

  private int port;
  private String ip;

  public ClientServer(Database database, AliasGenerationService aliasGenerationService, String ip, int port, int backlog) {
    this.port = port;
    this.ip = ip;
    try {
      httpServer = HttpServer.create(new InetSocketAddress(ip, port), backlog);
    } catch (IOException e) {
      throw new RuntimeException("Could not start http server on port " + port + " for IP address " + ip);
    }

    executorService = Executors.newFixedThreadPool(20);
    httpServer.setExecutor(executorService);

    httpServer.createContext("/", new ClientHttpHandler(database, aliasGenerationService));
  }

  public void start() {
    logger.info("Starting client server at port " + port + " for IP address " + ip);
    httpServer.start();
  }

  public void stop() {
    httpServer.stop(1);
    executorService.shutdownNow();
  }
}
