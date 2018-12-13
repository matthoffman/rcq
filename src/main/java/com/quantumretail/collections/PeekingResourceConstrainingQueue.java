package com.quantumretail.collections;

import com.quantumretail.constraint.ConstraintStrategy;
import com.quantumretail.rcq.predictor.TaskTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A ResourceConstrainingQueue implementation that tries to precede each poll(), remove() or take() with a peek() call,
 * in order to determine whether we have resources available for the item.
 * <p>
 * Concurrency note:
 * There are several important concurrency-related issues to keep in mind when using ResourceConstrainingQueue.
 * <p>
 * First and foremost, this implementation relies on {@link #peek()} to check if we have resources available to execute
 * the next task, without actually claiming that next task. Therefore it assumes that a peek() and a subsequent poll()
 * will return the same object. Of course, with multiple readers, this may well not be the case. Therefore, by default,
 * we have a global lock on reads:  each poll(), remove() or take() operation locks to try to ensure that a subsequent
 * peek() and take() returns the same object.
 * Three things to note about this implementation:
 * 1. If there are other consumers of the underlying queue other than this class, peek() and poll() may return
 * different objects and we may return items we do not have the resources to handle.
 * 2. If the underlying queue implementation is distributed, such that there are multiple readers on this queue on
 * different JVMs, the same applies: we may return items that we do not have the resources to handle.
 * 3. Since peek() is non-blocking, blocking calls (poll(timeout), take()) are implemented with a polling loop, with a
 * poll frequency governed by the "retryFrequencyMS" constructor argument.
 * <p>
 * In practice, the concurrent-access issue is only a problem when subsequent items vary widely in their resource needs,
 * but it's important to be aware of.
 * <p>
 * If strict == false in the constructor, we will *not* lock on reads. That means that two concurrent reads can return
 * two items, without ever checking to see if we have the resources available for the second item explicitly (the first
 * item will be checked twice instead).
 * In some cases, that might be preferable to locking: if resources are generally homogeneous, and high throughput is
 * important, the cost of occasionally checking the wrong task may be acceptable. However, strict is "true" by default.
 *
 * @param <T>
 */
public class PeekingResourceConstrainingQueue<T> extends ResourceConstrainingQueue<T> {
    private static final Logger log = LoggerFactory.getLogger(PeekingResourceConstrainingQueue.class);
    final private boolean strict;

    public PeekingResourceConstrainingQueue(BlockingQueue<T> delegate, ConstraintStrategy<T> constraintStrategy, long retryFrequencyMS, boolean strict, TaskTracker<T> taskTracker, long constrainedItemThresholdMS) {
        super(delegate, constraintStrategy, retryFrequencyMS, taskTracker, constrainedItemThresholdMS);
        this.strict = strict;
    }


    private boolean shouldLock() {
        return strict;
    }

    @Override
    protected T getItemNonBlocking(Supplier<T> onNoElementAvailable, Function<T, T> onInsufficientResources) {
        boolean locking = shouldLock();
        try {
            if (locking) {
                // perf note: synchronized(obj) and StripedLock are both more efficient than ReentrantLock for
                // most cases, but using ReentrantLock makes the "optionally strict" logic much simpler, and the
                // differences are not significant in this application.
                takeLock.lock();
            }
            T nextItem = delegate.peek();
            if (nextItem == null) {
                return onNoElementAvailable.get();
            } else if (shouldReturn(nextItem)) {
                // note that if nextItem == null, remove() here will throw an exception.
                // Note that we might be returning a *different item* than nextItem if we have multiple threads
                // accessing this concurrently and strict == false!
                // When strict == false, we're intentionally taking that risk to avoid locking.
                T item = trackIfNecessary(delegate.poll());
                if (item == null) return onNoElementAvailable.get();
                else return item;
            } else {
                return onInsufficientResources.apply(nextItem);
            }
        } finally {
            if (locking) {
                takeLock.unlock();
            }
        }
    }

    @Override
    protected T getItemBlocking(long timeout, TimeUnit unit) throws InterruptedException {
// we have to do a little extra work here because we may have to wait for some time before we have enough resources.
        // We make a reasonable effort to ensure that the combined wait-until-resources-are-available and poll time don't
        // exceed the desired timeout.
        long totalSleepNanos = 0;
        long startNanos = System.nanoTime();
        long timeoutNanos = unit.toNanos(timeout);
        // we treat "timeoutNanos < 0" as "no limit"
        while (timeoutNanos < 0 || totalSleepNanos < timeoutNanos) {
            try {
                return getItemNonBlocking(() -> {
                    throw new NoSuchElementException();
                }, (nextItem) -> {
                    if (shouldFail(nextItem)) return failForTooMayTries(nextItem);
                    else return null;
                });
            } catch (NoSuchElementException e) {
                // alas, either nothing available or we don't have the resources to execute it. Sleep, and try again.
                // sleep and retry
                sleep();
                totalSleepNanos = System.nanoTime() - startNanos;
            }
        }
        // if we got here, we timed out.
        return null;
    }
}
