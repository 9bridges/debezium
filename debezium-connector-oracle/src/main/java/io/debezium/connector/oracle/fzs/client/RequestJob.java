package io.debezium.connector.oracle.fzs.client;


import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;

public class RequestJob implements Runnable {
    public static final int REQUEST_MAX_SIZE = 4096;
    final static Logger log = LoggerFactory.getLogger(RequestJob.class);
    private final Socket socket;
    private final String serverName;

    private final String jobName;

    final private BlockingQueue<byte[]> blockingQueue;

    public RequestJob(String jobName, String serverName, Socket socket, BlockingQueue<byte[]> blockingQueue) {
        this.socket = socket;
        this.serverName = serverName;
        this.jobName = jobName;
        this.blockingQueue = blockingQueue;
    }

    @Override
    public void run() {
        Thread.currentThread().setName(jobName);
        try {
            final InputStream iStream = socket.getInputStream();
            final OutputStream oStream = socket.getOutputStream();
            socket.setSoTimeout(60000);
            log.info("[{}] socket timeout={}", jobName, socket.getSoTimeout());
            while (true) {
                //1.read
                byte[] requestBytes = new byte[REQUEST_MAX_SIZE];
                log.info("[{}] wait read REQ...", jobName);
                long start0 = System.currentTimeMillis();
                int readed;
                try {
                    readed = iStream.read(requestBytes, 0, REQUEST_MAX_SIZE);
                } catch (IOException e) {
                    if (e.getMessage().contains("Read timed out")) {
                        try {
                            Thread.sleep(2000);
                        } catch (InterruptedException ignored) {
                        }
                        continue;
                    }
                    throw e;
                }
                if (readed <= 0) {
                    log.warn("[{}] read readed={} break", jobName, readed);
                    break;
                }
                String request = new String(requestBytes, 0, readed);
                log.info("[{}] read REQ:{} readed={},delay={}ms", jobName, request, readed, (System.currentTimeMillis() - start0));
                Element element = DocumentHelper.parseText(request).getRootElement();
                String fzsNo = element.attributeValue("FzsNo");
                String size = element.attributeValue("SIZE");
                String zip = element.attributeValue("Zip", "0");
                String zipSize = element.attributeValue("ZipSize");
                //2.write
                Element command = DocumentHelper.createElement("DATARESULT");

                String asXML = command.addAttribute("Ok", "1").asXML();
                byte[] writeContent = asXML.getBytes();
                BytesUtils.writeBytes(oStream, writeContent, 0, writeContent.length);
                log.info("[{}] write RES:{},len={},delay={}ms", jobName, asXML, writeContent.length, (System.currentTimeMillis() - start0));
                //3.read data
                int readSize = Integer.parseInt(size);
                if ("1".equals(zip))
                    readSize = Integer.parseInt(zipSize);
                byte[] dataBytes = new byte[readSize];
                log.info("[{}] wait read DATA[{}]...", jobName, readSize);

                start0 = System.currentTimeMillis();
                readed = BytesUtils.readBytes(iStream, dataBytes, readSize);
                if (readed == 0) {
                    log.warn("[{}] read DATA:readed={}/{} continue", jobName, readed, readSize);
                    asXML = command.addAttribute("Ok", "1").asXML();
                    writeContent = asXML.getBytes();
                    BytesUtils.writeBytes(oStream, writeContent, 0, writeContent.length);
                    continue;
                }
                blockingQueue.put(dataBytes);
                log.info("[{}] read DATA:readed={}/{},delay={}ms", jobName, readed, readSize, (System.currentTimeMillis() - start0));
                //4.write
                asXML = command.addAttribute("Ok", "1").asXML();
                writeContent = asXML.getBytes();
                BytesUtils.writeBytes(oStream, writeContent, 0, writeContent.length);
                log.info("[{}] write RES:{},len={},delay={}ms", jobName, asXML, writeContent.length, (System.currentTimeMillis() - start0));
            }
        } catch (IOException | DocumentException e) {
            e.printStackTrace();
            log.error("[{}] error:{}", jobName, e.getMessage());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException ignored) {
                }
            }
        }
        log.info("[{}] exit", jobName);
    }
}
