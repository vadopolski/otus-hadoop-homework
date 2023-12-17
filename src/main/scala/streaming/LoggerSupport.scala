package streaming

import org.slf4j.LoggerFactory

import java.util.logging.Logger

trait LoggerSupport {

//  protected final val LOGGER: Logger = LoggerFactory.getLogger(getSimpleName(this.getClass))
//
//  /**
//   * This method is utilized to log code block duration and starting, finishing time.
//   *
//   * @param trackingCodeBlock Code block for logging.
//   * @param message           Short message for log simplicity.
//   */
//  protected def withDurationLogging(trackingCodeBlock: => Unit, message: String = null): Unit = {
//    val uuid = UUID.randomUUID().toString
//    val start = System.currentTimeMillis()
//
//    LOGGER.info(s"[logUUID: $uuid] ${message ? "No info"}. Started at ${DateTime.now()}")
//    trackingCodeBlock
//    LOGGER.info(s"[logUUID: $uuid] finished at ${DateTime.now()}. Duration -> ${formattedDuration(start)}")
//  }
//
//  /**
//   * This method is utilized to log code block duration with custom implementation.
//   *
//   * @param trackingCodeBlock Code block for logging.
//   * @param durationCallback  Callback function, that receives formatted duration.
//   * @return duration of the tracking code block execution.
//   */
//  protected def withDurationHandler(trackingCodeBlock: => Unit, durationCallback: String => Unit): String = {
//    val start = System.currentTimeMillis()
//    trackingCodeBlock
//
//    val duration = formattedDuration(start)
//    durationCallback(duration)
//    duration
//  }
}
