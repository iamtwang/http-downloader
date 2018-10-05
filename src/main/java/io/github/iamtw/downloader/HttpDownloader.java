package io.github.iamtw.downloader;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Download HTTP file with multiple threads.
 * 
 * @author iamtw
 *
 */
public class HttpDownloader {

	private static final Logger logger = LoggerFactory.getLogger(HttpDownloader.class);

	// TCP connection timeout milliseconds
	private static final int TIME_OUT = 5000;
	// TCP connection max Retry
	private static final int MAX_RETRY = 10;

	private static final String TEMPLATE_INFO = "Speed: %d KB/s, Downloaded: %d KB (%.2f%%), Threads: %d";

	private static final String KEY_RANGE = "range";

	private boolean resumable;
	private URL url;
	private File localFile;
	private long[] endPoint;
	private Object waiting = new Object();
	private AtomicLong downloadedBytes = new AtomicLong(0);
	private AtomicInteger aliveThreads = new AtomicInteger(0);
	private boolean singleThread;
	private long fileSize = 0;
	private int threadsNum = 7;

	public static void main(String[] args) throws IOException {
		if (args == null || args.length == 0) {
			throw new RuntimeException("please specific the url");
		}
		String url = args[0];
		String last = url.substring(url.lastIndexOf('-') + 1);
		new HttpDownloader(url, "D:/Code/" + last, 7).get();
	}

	/**
	 * Constructor
	 * 
	 * @param Url
	 * @param localPath
	 * @param threadNum
	 * @throws MalformedURLException
	 */
	public HttpDownloader(String Url, String localPath, int threadNum) throws MalformedURLException {
		this.url = new URL(Url);
		this.localFile = new File(localPath);
		this.threadsNum = threadNum;
	}

	/**
	 * Start to download the file
	 * 
	 * @throws IOException
	 */
	public void get() throws IOException {
		long startTime = System.currentTimeMillis();

		resumable = supportHttpPartialDownload();

		singleThread = !resumable || (threadsNum == 1);

		if (singleThread) {
			new Worker(0, 0, fileSize - 1).start();
		} else {
			endPoint = new long[threadsNum + 1];
			long block = fileSize / threadsNum;
			for (int i = 0; i < threadsNum; i++) {
				endPoint[i] = block * i;
			}
			endPoint[threadsNum] = fileSize;
			for (int i = 0; i < threadsNum; i++) {
				new Worker(i, endPoint[i], endPoint[i + 1] - 1).start();
			}
		}

		startDownloadMonitor();

		// start the download monitor
		try {
			synchronized (waiting) {
				waiting.wait();
			}
		} catch (InterruptedException e) {
			logger.error("Download interrupted.");
		}

		cleanTempFile();

		long timeElapsed = System.currentTimeMillis() - startTime;
		logger.info("* File successfully downloaded.");
		logger.info("* Time used: {} s, Average speed: {} KB/s", timeElapsed / 1000.0,
				downloadedBytes.get() / timeElapsed);
	}

	// check if support download
	private boolean supportHttpPartialDownload() throws IOException {
		HttpURLConnection con = (HttpURLConnection) url.openConnection();
		con.setRequestProperty(KEY_RANGE, "bytes=0-");
		int resCode = 0;
		int retry = 0;
		while (true && (retry < MAX_RETRY)) {
			retry += 1;
			try {
				con.connect();
				TimeUnit.MILLISECONDS.sleep(100L);
				fileSize = con.getContentLengthLong();
				resCode = con.getResponseCode();
				con.disconnect();
				break;
			} catch (Exception e) {
				System.out.println("Retry to connect due to connection problem.");
			}
		}
		logger.debug("Support resume download is {}", (HttpURLConnection.HTTP_PARTIAL == resCode));
		return HttpURLConnection.HTTP_PARTIAL == resCode;

	}

	// Start the download monitor
	public void startDownloadMonitor() {
		Thread downloadMonitor = new Thread(() -> {
			long prev = 0;
			long curr = 0;
			while (true) {
				try {
					TimeUnit.SECONDS.sleep(10L);
				} catch (InterruptedException e) {
				}

				curr = downloadedBytes.get();
				String info = String.format(TEMPLATE_INFO, (curr - prev) >> 10, curr >> 10,	curr / (float) fileSize * 100, aliveThreads.get());
				logger.debug(info);
				prev = curr;

				if (aliveThreads.get() == 0) {
					synchronized (waiting) {
						waiting.notifyAll();
					}
				}
			}
		});

		downloadMonitor.setDaemon(true);
		downloadMonitor.start();

	}

	// clear temp file
	public void cleanTempFile() throws IOException {
		if (singleThread) {
			Files.move(Paths.get(localFile.getAbsolutePath() + ".0.tmp"), Paths.get(localFile.getAbsolutePath()),
					StandardCopyOption.REPLACE_EXISTING);
		} else {
			merge();
			logger.debug("* Temp file merged.");
		}
	}

	// merge file
	private void merge() {
		try (OutputStream out = new FileOutputStream(localFile)) {
			byte[] buffer = new byte[1024];
			int size;
			for (int i = 0; i < threadsNum; i++) {
				String tmpFile = localFile.getAbsolutePath() + "." + i + ".tmp";
				InputStream in = new FileInputStream(tmpFile);
				while ((size = in.read(buffer)) != -1) {
					out.write(buffer, 0, size);
				}
				in.close();
				Files.delete(Paths.get(tmpFile));
			}
		} catch (IOException ioe) {
			logger.error("failed to merge file {}", ioe);
		}
	}

	class Worker extends Thread {
		private int id;
		private long start;
		private long end;
		private OutputStream out;

		public Worker(int id, long start, long end) {
			this.id = id;
			this.start = start;
			this.end = end;
			aliveThreads.incrementAndGet();
		}

		@Override
		public void run() {
			boolean success = false;
			int retry = 0;
			while (true && (retry < MAX_RETRY)) {
				retry += 1;
				success = download();
				if (success) {
					logger.debug("Downloaded part {} with {}", (id + 1), this.getName());
					break;
				} else {
					logger.debug("Retry to download part {} with {}", (id + 1), this.getName());
				}
			}
			aliveThreads.decrementAndGet();
		}

		private boolean download() {
			try {
				HttpURLConnection con = (HttpURLConnection) url.openConnection();
				con.setRequestProperty(KEY_RANGE, String.format("bytes=%d-%d", start, end));
				logger.debug("bytes={}  {} with {}", start, end, this.getName());
				con.setConnectTimeout(TIME_OUT);
				con.setReadTimeout(TIME_OUT);
				con.connect();
				TimeUnit.MILLISECONDS.sleep(100L);
				int partSize = con.getHeaderFieldInt("Content-Length", -1);
				if (partSize != end - start + 1)
					return false;
				if (out == null)
					out = new FileOutputStream(localFile.getAbsolutePath() + "." + id + ".tmp");
				try (InputStream in = con.getInputStream()) {
					byte[] buffer = new byte[1024];
					int size;
					while (start <= end && (size = in.read(buffer)) > 0) {
						start += size;
						downloadedBytes.addAndGet(size);
						out.write(buffer, 0, size);
						out.flush();
					}
					con.disconnect();
					if (start <= end)
						return false;
					else
						out.close();
				}
				return true;
			} catch (IOException e) {
				logger.error("Part {} IO error.", (id + 1));
				return false;
			} catch (Exception e) {
				logger.error("Part {} error.", (id + 1));
				return false;
			}

		}
	}

}