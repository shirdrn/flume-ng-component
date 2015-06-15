package org.shirdrn.flume.sink;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.serialization.EventSerializer;
import org.apache.flume.serialization.EventSerializerFactory;
import org.apache.flume.sink.AbstractSink;
import org.apache.flume.sink.RollingFileSink;
import org.shirdrn.flume.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class SharpTimeRollingFileSink extends AbstractSink implements Configurable {

	private static final Logger logger = LoggerFactory.getLogger(RollingFileSink.class);
	private static final int defaultBatchSize = 100;

	private int batchSize = defaultBatchSize;

	private File directory;
	private OutputStream outputStream;

	private String serializerType;
	private Context serializerContext;
	private EventSerializer serializer;
	private volatile boolean stop = false;
	private SinkCounter sinkCounter;

	private PathManager pathController;

	private String filePrefix;
	private String fileSuffix;
	private String filePattern;
	private volatile boolean shouldRotate;
	private String forceRotateType;

	@Override
	public void configure(Context context) {

		String directory = context.getString("sink.directory");

		serializerType = context.getString("sink.serializer", "TEXT");
		serializerContext = new Context(context.getSubProperties("sink." + EventSerializer.CTX_PREFIX));

		Preconditions.checkArgument(directory != null, "Directory may not be null");
		Preconditions.checkNotNull(serializerType, "Serializer type is undefined");

		batchSize = context.getInteger("sink.batchSize", defaultBatchSize);

		pathController = new PathManager();
		this.directory = new File(directory);

		if (sinkCounter == null) {
			sinkCounter = new SinkCounter(getName());
		}

		filePrefix = context.getString("sink.file.prefix", "prefix");
		fileSuffix = context.getString("sink.file.suffix", ".txt");
		filePattern = context.getString("sink.file.pattern", "yyyyMMddHHmmss");
		// H-hour; M-minute; S-second
		forceRotateType = context.getString("sink.file.force.rotate.type", "H").toUpperCase();
	}

	@Override
	public void start() {
		logger.info("Starting {}...", this);
		sinkCounter.start();
		super.start();

		pathController.setBaseDirectory(directory);

		// start sharp time rotator
		Thread sharpTimeRotator = new SharpTimeRotator();
		sharpTimeRotator.start();
		logger.info("Start sharp time roller thread: " + sharpTimeRotator);
		logger.info("RichRollingFileSink {} started.", getName());
	}

	@Override
	public Status process() throws EventDeliveryException {
		if (shouldRotate) {
			logger.debug("Time to rotate {}", pathController.getCurrentFile());
			if (outputStream != null) {
				logger.debug("Closing file {}", pathController.getCurrentFile());

				try {
					serializer.flush();
					serializer.beforeClose();
					outputStream.close();
					sinkCounter.incrementConnectionClosedCount();
					shouldRotate = false;
				} catch (IOException e) {
					sinkCounter.incrementConnectionFailedCount();
					throw new EventDeliveryException("Unable to rotate file " + pathController.getCurrentFile() + " while delivering event", e);
				} finally {
					serializer = null;
					outputStream = null;
				}
				logger.debug("Start to rotate, current:" + TimeUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss"));
				pathController.rotate();
				logger.debug("Finish to rotate, current:" + TimeUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss"));
			}
		}

		if (outputStream == null) {
			try {
				Thread.sleep(5);
				File currentFile = pathController.getCurrentFile();
				logger.debug("Opening output stream for file {}", currentFile);
				outputStream = new BufferedOutputStream(new FileOutputStream(currentFile));
				serializer = EventSerializerFactory.getInstance(serializerType, serializerContext, outputStream);
				serializer.afterCreate();
				sinkCounter.incrementConnectionCreatedCount();
			} catch (Exception e) {
				sinkCounter.incrementConnectionFailedCount();
				throw new EventDeliveryException("Failed to open file " + pathController.getCurrentFile() + " while delivering event", e);
			}
		}

		Channel channel = getChannel();
		Transaction transaction = channel.getTransaction();
		Event event = null;
		Status result = Status.READY;

		try {
			transaction.begin();
			int eventAttemptCounter = 0;
			for (int i = 0; i < batchSize; i++) {
				event = channel.take();
				if (event != null) {
					sinkCounter.incrementEventDrainAttemptCount();
					eventAttemptCounter++;
					serializer.write(event);
				} else {
					// No events found, request back-off semantics from runner
					result = Status.BACKOFF;
					break;
				}
			}
			serializer.flush();
			outputStream.flush();
			transaction.commit();
			sinkCounter.addToEventDrainSuccessCount(eventAttemptCounter);
		} catch (Exception ex) {
			transaction.rollback();
			throw new EventDeliveryException("Failed to process transaction", ex);
		} finally {
			transaction.close();
		}

		return result;
	}

	private class SharpTimeRotator extends Thread {

		@Override
		public void run() {
			while (!stop) {
				try {
					if (!shouldRotate) {
						logger.debug("Decide next sharp time, current:" + TimeUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss"));
						long waitTime = nextSharpTimeDistance();
						Thread.sleep(Math.max(1L, waitTime - 3));
						logger.debug("Trigger rotation, current:" + TimeUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss"));
						shouldRotate = true;
					} else {
						Thread.sleep(200);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		private long nextSharpTimeDistance() {
			long waitTime = 0;
			if (forceRotateType.equalsIgnoreCase("H")) {
				// hour
				waitTime = (int) TimeUtils.getNextHour(new Date());
			} else if (forceRotateType.equalsIgnoreCase("M")) {
				// minute
				waitTime = (int) TimeUtils.getNextMinute(new Date());
			}
			return waitTime;
		}
	}

	@Override
	public void stop() {
		logger.info("RollingFile sink {} stopping...", getName());
		sinkCounter.stop();
		super.stop();

		if (outputStream != null) {
			logger.debug("Closing file {}", pathController.getCurrentFile());

			try {
				serializer.flush();
				serializer.beforeClose();
				outputStream.close();
				sinkCounter.incrementConnectionClosedCount();
			} catch (IOException e) {
				sinkCounter.incrementConnectionFailedCount();
				logger.error("Unable to close output stream. Exception follows.", e);
			} finally {
				outputStream = null;
				serializer = null;
			}
		}
		stop = true;
		logger.info("RollingFile sink {} stopped. Event metrics: {}", getName(), sinkCounter);
	}

	public File getDirectory() {
		return directory;
	}

	public void setDirectory(File directory) {
		this.directory = directory;
	}

	final class PathManager {

		private File baseDirectory;
		private AtomicInteger fileIndex;

		private File currentFile;

		public PathManager() {
			fileIndex = new AtomicInteger();
		}

		public File nextFile() {
			String fileName = TimeUtils.format(new Date(), filePattern);
			String file = filePrefix + "-" + fileName + "-" + fileIndex.incrementAndGet() + fileSuffix;
			currentFile = new File(baseDirectory, file);
			return currentFile;
		}

		public File getCurrentFile() {
			if (currentFile == null) {
				return nextFile();
			}
			return currentFile;
		}

		public void rotate() {
			currentFile = null;
		}

		public File getBaseDirectory() {
			return baseDirectory;
		}

		public void setBaseDirectory(File baseDirectory) {
			this.baseDirectory = baseDirectory;
		}

		public AtomicInteger getFileIndex() {
			return fileIndex;
		}

	}

}
