package com.quantumretail.collections;

import com.quantumretail.constraint.ConstraintStrategy;
import com.quantumretail.rcq.predictor.TaskTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A subclass of ResourceConstrainingQueue that holds at most one item locally until there are sufficient resources to
 * execute it.
 * <p>
 * Pulling an item off of the queue and holding it locally until we have available resources could have significant
 * implications, depending on the underlying queue implementation.
 * <p>
 * Since that item has been removed from the underlying queue, any durability or HA guarantees provided by that queue
 * (say, in the case of a queue backed by a distributed, fault-tolerant datastore) may be lost for that one item: the
 * datastore may consider that item "done" and stop tracking it, thus leaving the possibility that the task is lost in
 * the event of sudden node failure.
 *
 * <p>
 * Items will only be buffered in response to a read operation (we don't eagerly fetch from the underlying queue) and
 * only when the resource constraint check fails for that item.
 * <p>
 * <p>
 * Another concurrency-related note:
 * <p>
 * If there are tasks available in the underlying queue, but we do not yet have the resources to hand them out, we do
 * not guarantee ordering between competing consumers. That is, if multiple threads are attempting to read from this
 * queue, but we are waiting to have the available resources to hand out the next task, which thread actually gets the
 * task when resources do become available is undefined. There is currently no "fair" mode for resource-contented waits.
 * However, if we are waiting for items to become available on the underlying queue, ordering *is* guaranteed, provided
 * the underlying queue guarantees order. The first caller should get the first item.
 * <p>
 *
 * @param <T>
 */
public class BufferingResourceConstrainingQueue<T> extends ResourceConstrainingQueue<T> {
    private static final Logger log = LoggerFactory.getLogger(BufferingResourceConstrainingQueue.class);

    // buffered item, used if shouldBuffer == true
    private volatile T buffer;

    /**
     * Use {@link ResourceConstrainingQueue#builder()} to construct a queue.
     *
     * @param delegate
     * @param constraintStrategy
     * @param retryFrequencyMS
     * @param taskTracker
     * @param constrainedItemThresholdMS
     */
    public BufferingResourceConstrainingQueue(BlockingQueue<T> delegate, ConstraintStrategy<T> constraintStrategy, long retryFrequencyMS, TaskTracker<T> taskTracker, long constrainedItemThresholdMS) {
        super(delegate, constraintStrategy, retryFrequencyMS, taskTracker, constrainedItemThresholdMS);
    }

    /**
     * Get an item, using an intermediate buffer to store items that we do not yet have the resources to execute.
     * Ideally, we want to pull off no more than 1 task from the queue at a time before we know for sure we have the
     * resources to execute it. That way if the queue is backed by a distributed queue, for example, we ensure as many
     * items are available for other workers as possible.
     * <p>
     * There's some duplication between this and pollWithBuffer(timeout), but behavior is sufficiently different in the
     * non-blocking case that it seemed clearer to keep them separate.
     *
     * @param onNoElementAvailable    what to return when there's nothing available
     * @param onInsufficientResources what to return when there's something available, but we don't have sufficient
     *                                resources to execute it.
     * @return an item if present, or the value returned by the appropriate input function/supplier otherwise
     */
    protected T getItemNonBlocking(Supplier<T> onNoElementAvailable, Function<T, T> onInsufficientResources) {
        final ReentrantLock lock = this.takeLock;
        lock.lock();
        try {
            // pull the next item into the buffer if possible (without waiting)
            fillBufferIfEmpty();
            if (buffer == null) return onNoElementAvailable.get();
            else if (constraintStrategy.shouldReturn(buffer)) {
                T item = buffer;
                buffer = null;
                return item;
            } else {
                // there's an item in the buffer, but we shouldn't return it yet
                return onInsufficientResources.apply(buffer);
            }
        } finally {
            lock.unlock();
        }
    }


    /**
     * Get an item, using an intermediate buffer to store items that we do not yet have the resources to execute.
     * <p>
     * This used to be implemented with a separate thread and a monitor to ensure that we weren't blocking during a poll(),
     * but I decided to go with a simple polling approach, like in PeekingResourceConstrainingQueue. It's much simpler
     * and easier to read, at the expense of a slightly worse performance profile and a bit of poll delay. We were already
     * polling in the case where the item had been fetched, but we were waiting for available resources to execute it.
     * So the tradeoff seemed worth it, but we can go back to the separate thread if necessary.
     *
     * <p>
     * Ideally, we want to pull off no more than 1 task from the queue at a time before we know for sure we have the
     * resources to execute it. That way if the queue is backed by a distributed queue, for example, we ensure as many
     * items are available for other workers as possible.
     * <p>
     * There's some duplication between this and getWithBuffer(timeout), but behavior is sufficiently different in the
     * non-blocking case that it seemed clearer to keep them separate.
     * <p>
     * If timeout < 0, it is treated as "no timeout"
     *
     * @param timeout if < 0, it is treated as "no timeout". Otherwise, treated as a best-effort max wait.
     * @return an item if present, or the value returned by the appropriate input function/supplier otherwise
     */
    protected T getItemBlocking(long timeout, TimeUnit unit) throws InterruptedException {
        // we have to do a little extra work here because we may have to wait for some time before we have enough resources.
        // We make a reasonable effort to ensure that the combined wait-until-resources-are-available and poll time don't
        // exceed the desired timeout.
        final long startNanos = System.nanoTime();
        final Optional<Long> timeoutNanos;
        if (timeout >= 0) {
            timeoutNanos = Optional.of(unit.toNanos(timeout));
        } else {
            timeoutNanos = Optional.empty();
        }
        final ReentrantLock lock = this.takeLock;
        // loop only until timeout, if timeout is present
        while (!timeoutNanos.isPresent() || remainingTime(startNanos, timeoutNanos.get()) > 0) {
            lock.lockInterruptibly();
            try {
                log.trace("Attempting to fill buffer");
                fillBufferIfEmpty();
                log.trace("Fill buffer complete");
                if (buffer == null) {
                    // either there was no item available within the timeout, or we were signaled spuriously, or someone
                    // else grabbed our buffered item before we reacquired the lock
                    log.trace("..but nothing available");
                    // loop back around again. If we've hit our timeout, we'll exit out of the loop.
                    continue;
                }

                // ok, now let's see if we have the resources to execute it.
                if (constraintStrategy.shouldReturn(buffer)) {
                    T item = buffer;
                    buffer = null;
                    return item;
                } else {
                    // there's something in the buffer, but we don't yet have the resources to execute it.
                    // Fall through out of the try block (so we release the lock, so that other threads don't block
                    // for non-blocking calls) and sleep, and try again.
                    if (shouldFail(buffer)) {
                        T item = buffer;
                        buffer = null;
                        return failForTooMayTries(item);
                    }
                }
            } finally {
                lock.unlock();
            }
            sleep();
        }
        // if we get here, then nothing was available within the specified wait time.
        return null;
    }

    /**
     * must have lock before calling this method
     */
    private void fillBufferIfEmpty() {
        if (buffer == null) {
            buffer = delegate.poll();
        }
    }

    private long remainingTime(long startNanos, long timeoutNanos) {
        long elapsed = System.nanoTime() - startNanos;
        return timeoutNanos - elapsed;
    }

}
