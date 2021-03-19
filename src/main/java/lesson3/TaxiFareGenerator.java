package lesson3;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * This SourceFunction generates a data stream of TaxiFare records that include event time
 * timestamps.
 *
 * <p>The stream is generated in order, and it includes Watermarks.
 *
 */
public class TaxiFareGenerator implements SourceFunction<TaxiFare> {

    private volatile boolean running = true;

    @Override
    public void run(SourceContext<TaxiFare> ctx) throws Exception {

        long id = 1;

        while (running) {
            TaxiFare fare = new TaxiFare(id);
            id += 1;

            ctx.collectWithTimestamp(fare, fare.getEventTime());
            ctx.emitWatermark(new Watermark(fare.getEventTime()));

            // match our event production rate to that of the TaxiRideGenerator
            Thread.sleep(TaxiRideGenerator.SLEEP_MILLIS_PER_EVENT);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
