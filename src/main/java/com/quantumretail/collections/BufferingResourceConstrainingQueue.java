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

public class BufferingResourceConstrainingQueue<T> extends ResourceConstrainingQueue<T> {
    private static final Logger log = LoggerFactory.getLogger(BufferingResourceConstrainingQueue.class);

    // we poll the underlying queue in a separate thread, so that we can avoid blocking other reads
    private final ExecutorService fillThread = Executors.newSingleThreadExecutor(new ResourceConstrainingQueues.NameableDaemonThreadFactory("queue-fill-thread-%d"));
    // buffered item, used if shouldBuffer == true
    private volatile T buffer;

    // We also don't want a blocking read (take(), poll(timeout)) to block a non-blocking poll() while we wait for
    // another queue read operation to complete (when filling == true). Waiting on this condition allows us to
    // release the lock while we wait for other queue-reading operations to complete.
    private Condition fillComplete = takeLock.newCondition();

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
     * Get an item, using an intermediate buffer to store items that we do not yet have the resources to execute. Uses
     * a separate thread to poll the underlying queue in order to avoid blocking other threads.
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
                log.debug("Attempting to fill buffer");
                fillBufferIfEmpty(timeoutNanos.map((t) -> remainingTime(startNanos, t)));
                log.debug("Fill buffer complete");
                if (buffer == null) {
                    // either there was no item available within the timeout, or we were signaled spuriously, or someone
                    // else grabbed our buffered item before we reacquired the lock
                    log.debug("..but nothing available");
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
     * must have lock before calling this method!
     */
    private void fillBufferIfEmpty(Optional<Long> timeoutNanos) throws InterruptedException {
        if (buffer == null && timeoutNanos.orElse(1L) > 0) {
            // we do this in a separate thread so that we can release our lock (via fillComplete.await()). That
            // prevents this blocking poll() call from blocking other non-blocking reads.
            fillThread.execute(() -> {
                ReentrantLock lock = this.takeLock;
                log.debug("Fill thread waiting for lock");
                lock.lock();
                log.debug("Fill thread acquired lock");
                try {
                    if (buffer == null) {
                        log.debug("Fill thread: buffer empty, polling underlying queue");
                        if (timeoutNanos.isPresent()) {
                            buffer = delegate.poll(timeoutNanos.get(), TimeUnit.NANOSECONDS);
                        } else {
                            buffer = delegate.take();
                        }
                        log.debug("Fill thread: poll complete. Buffer is now {}", buffer);
                    }
                } catch (InterruptedException e) {
                    // typically happens because we're shutting down. Release our lock and move on.
                    log.warn("Interrupted while waiting to read from the underlying queue");
                } finally {
                    // whether it succeeded or not, wake somebody up to check on it.
                    fillComplete.signal();
                    lock.unlock();
                }
            });
            // we don't just wait for the future, because we want to release our locks.
            if (timeoutNanos.isPresent()) {
                fillComplete.await(timeoutNanos.get(), TimeUnit.NANOSECONDS);
            } else {
                fillComplete.await();
            }
        }
    }

    /**
     * must have lock before calling this method
     */
    private void fillBufferIfEmpty() {
        if (buffer == null) {
            buffer = delegate.poll();
            fillComplete.signal();
        }
    }

    private long remainingTime(long startNanos, long timeoutNanos) {
        long elapsed = System.nanoTime() - startNanos;
        return timeoutNanos - elapsed;
    }

}
