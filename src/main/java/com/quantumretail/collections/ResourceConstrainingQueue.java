package com.quantumretail.collections;

import com.quantumretail.metrics.MetricsAware;
import com.quantumretail.constraint.ConstraintStrategies;
import com.quantumretail.constraint.ConstraintStrategy;
import com.quantumretail.rcq.predictor.TaskTracker;
import com.quantumretail.rcq.predictor.TaskTrackers;
import com.codahale.metrics.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Note that this resource-constraining behavior ONLY occurs on {@link #poll()}, {@link #take()} and {@link #remove()}.
 * Other access methods like {@link #peek()}, {@link #iterator()}, {@link #toArray()}, and so on will bypass the
 * resource-constraining behavior.
 * <p/>
 *
 * This class has 3 distinct modes of operation, which represent different tradeoffs in the face of concurrent access.
 *
 * Concurrency notes:
 * There are several important concurrency-related issues to keep in mind when using ResourceConstrainingQueue.
 *
 * First and foremost, this implementation relies on {@link #peek()} to check if we have resources available to execute
 * the next task, without actually claiming that next task. Therefore it assumes that a peek() and a subsequent poll()
 * will return the same object. Of course, with multiple readers, this may well not be the case. Therefore, by default,
 * we have a global lock on reads:  each poll(), remove() or take() operation locks to try to ensure that a subsequent
 * peek() and take() returns the same object.
 * Three things to note about this implementation:
 *  1. If there are other consumers of the underlying queue outside of this class, peek() and poll() may return
 *  different objects and we may return items we do not have the resources to handle.
 *  2. If the underlying queue implementation is distributed, meaning there are multiple readers on this queue on
 *  different JVMs, the same applies: a consecutive peek() and poll() can return different objects, so we may return
 *  items that we do not have the resources to handle.
 *  3. Since peek() is non-blocking, blocking calls (poll(timeout), take()) are implemented with a polling loop, with a
 *  poll frequency governed by the "retryFrequencyMS" argument.
 *
 * In practice, the concurrent-access issue is only a problem when subsequent items vary widely in their resource needs,
 * but it's important to be aware of.
 *
 * Another concurrency-related note:
 *
 * If there are tasks available in the underlying queue, but we do not yet have the resources to hand them out, we do
 * not guarantee ordering between competing consumers. That is, if multiple threads are attempting to read from this
 * queue, but we are waiting to have the available resources to hand out the next task, which thread actually gets the
 * task when resources do become available is undefined. There is currently no "fair" mode for resource-contented waits.
 * However, if we are waiting for items to become available on the underlying queue, ordering *is* guaranteed, provided
 * the underlying queue guarantees order. The first caller should get the first item.
 *
 * If strict == false in the constructor, we will *not* lock on reads. That means that two concurrent reads can return
 * two items, without ever checking to see if we have the resources available for the second item explicitly (the first
 * item will be checked twice instead).
 * In some cases, that might be preferable to locking: if resources are generally homogeneous, and high throughput is
 * important, the cost of occasionally checking the wrong task may be acceptable. However, strict is "true" by default.
 *
 * Track https://github.com/matthoffman/rcq/issues/1 for another alternative implementation with different correctness
 * guarantees that doesn't rely on peek().
 *
 */
public class ResourceConstrainingQueue<T> implements BlockingQueue<T>, MetricsAware {
    private static final Logger log = LoggerFactory.getLogger(ResourceConstrainingQueue.class);

    public static <T> ResourceConstrainingQueueBuilder<T> builder() {
        return new ResourceConstrainingQueueBuilder<>();
    }

    private final ExecutorService fillThread;
    private boolean failAfterAttemptThresholdReached = false;

    public static final long DEFAULT_POLL_FREQ_MS = 100L;
    //the default will try for 10 mins  (default poll freq = 100L)
    protected static final long DEFAULT_CONSTRAINED_ITEM_THRESHOLD_MS = (10 * 60 * 1000) / DEFAULT_POLL_FREQ_MS;

    final BlockingQueue<T> delegate;
    long retryFrequencyMS = DEFAULT_POLL_FREQ_MS;
    long constrainedItemThresholdMS = DEFAULT_CONSTRAINED_ITEM_THRESHOLD_MS;

    final ConstraintStrategy<T> constraintStrategy;

    final TaskTracker<T> taskTracker;
    final TaskAttemptCounter taskAttemptCounter;

    private Meter trackedRemovals = null;
    private Meter additions = null;
    private Counter pendingItems = null;
    private Meter sleeps = null;

    final private boolean strict;
    // this is the lock we'll use if buffer == true or strict == true.
    ReentrantLock takeLock = new ReentrantLock(true);

    final private boolean shouldBuffer;

    // buffered item, used if shouldBuffer == true
    private volatile T buffer;

    // We also don't want a blocking read (take(), poll(timeout)) to block a non-blocking poll() while we wait for
    // another queue read operation to complete (when filling == true). Waiting on this condition allows us to
    // release the lock while we wait for other queue-reading operations to complete.
    private Condition fillComplete = takeLock.newCondition();

    /**
     * Build a ResourceConstrainingQueue using all default options.
     * If you want to override some defaults, but not all, use the ResourceConstrainingQueueBuilder; it's much easier.
     */
    public ResourceConstrainingQueue() {
        this(new LinkedBlockingQueue<>(), TaskTrackers.defaultTaskTracker(), DEFAULT_POLL_FREQ_MS);
    }

    public ResourceConstrainingQueue(BlockingQueue<T> delegate, TaskTracker<T> taskTracker, long defaultPollFreq) {
        this(delegate, ConstraintStrategies.defaultConstraintStrategy(taskTracker), defaultPollFreq, true, taskTracker);
    }

    public ResourceConstrainingQueue(BlockingQueue<T> delegate, ConstraintStrategy<T> constraintStrategy, long retryFrequencyMS, boolean strict) {
        this(delegate, constraintStrategy, retryFrequencyMS, strict, null);
    }

    public ResourceConstrainingQueue(BlockingQueue<T> delegate, ConstraintStrategy<T> constraintStrategy, long retryFrequencyMS, boolean strict, TaskTracker<T> taskTracker) {
        this(delegate, constraintStrategy, retryFrequencyMS, strict, true, taskTracker, DEFAULT_CONSTRAINED_ITEM_THRESHOLD_MS);
    }

    public ResourceConstrainingQueue(BlockingQueue<T> delegate, ConstraintStrategy<T> constraintStrategy, long retryFrequencyMS, boolean strict, boolean shouldBuffer, TaskTracker<T> taskTracker, long constrainedItemThresholdMS) {

        this.delegate = delegate;
        this.retryFrequencyMS = retryFrequencyMS;
        this.constraintStrategy = constraintStrategy;
        this.taskTracker = taskTracker;
        this.strict = strict;
        this.constrainedItemThresholdMS = constrainedItemThresholdMS;
        this.taskAttemptCounter = new TaskAttemptCounter();
        this.shouldBuffer = shouldBuffer;
        if (this.shouldBuffer) {
            fillThread = Executors.newSingleThreadExecutor(new ResourceConstrainingQueues.NameableDaemonThreadFactory("queue-fill-thread-%d"));
        } else {
            fillThread = null;
        }
    }

    protected T trackIfNecessary(T item) {
        // if item == null, nothing to track.
        if (item == null) return null;

        if (trackedRemovals != null) {
            trackedRemovals.mark();
        }
        if (pendingItems != null) {
            pendingItems.dec();
        }
        if (taskAttemptCounter != null) {
            taskAttemptCounter.removeConstrained(item);
        }
        if (taskTracker != null) {
            return taskTracker.register(item);
        } else {
            return item;
        }
    }

    public boolean add(T t) {
        markAddition();
        return delegate.add(t);
    }

    private void markAddition() {
        if (pendingItems != null) {
            pendingItems.inc();
        }
        if (additions != null) {
            additions.mark();
        }
    }

    public boolean offer(T t) {
        markAddition();
        return delegate.offer(t);
    }

    /**
     * Retrieves and removes the head of this queue.  This method differs
     * from {@link #poll poll} only in that it throws an exception if this
     * queue is empty.
     *
     * See the concurrency notes in the class-level javadoc for important notes about the accuracy and threadsafety of
     * this method.
     *
     * @return the head of this queue
     * @throws NoSuchElementException if this queue is empty
     * @throws InsufficientResourcesException if we do not have sufficient resources for the next element
     */
    @Override
    public T remove() {
        if (shouldBuffer) {
            return getItemWithBuffer(() -> {
                throw new NoSuchElementException();
            }, (n) -> {
                throw new InsufficientResourcesException(n);
            });
        } else {
            return getItemWithoutBuffer(Queue::remove, () -> {
                throw new NoSuchElementException();
            }, (n) -> {
                throw new InsufficientResourcesException(n);
            });
        }
    }

    /**
     * Abstracting out the poll, take, and remove methods, for the non-buffering use case.
     * @param getFromDelegate
     * @param onNoElementAvailable
     * @param onInsufficientResources
     * @return
     */
    private T getItemWithoutBuffer(Function<BlockingQueue<T>, T> getFromDelegate, Supplier<T> onNoElementAvailable, Function<T, T> onInsufficientResources) {
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
                return trackIfNecessary(getFromDelegate.apply(delegate));
            } else {
               return onInsufficientResources.apply(nextItem);
            }
        } finally {
            if (locking) {
                takeLock.unlock();
            }
        }

    }

    /**
     * Get an item, using an intermediate buffer to store items that we do not yet have the resources to execute.
     * Ideally, we want to pull off no more than 1 task from the queue at a time before we know for sure we have the
     * resources to execute it. That way if the queue is backed by a distributed queue, for example, we ensure as many
     * items are available for other workers as possible.
     *
     * There's some duplication between this and pollWithBuffer(timeout), but behavior is sufficiently different in the
     * non-blocking case that it seemed clearer to keep them separate.
     * @param onNoElementAvailable what to return when there's nothing available
     * @param onInsufficientResources what to return when there's something available, but we don't have sufficient
     *                                resources to execute it.
     * @return an item if present, or the value returned by the appropriate input function/supplier otherwise
     */
    private T getItemWithBuffer(Supplier<T> onNoElementAvailable, Function<T, T> onInsufficientResources) {
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
     *
     * Ideally, we want to pull off no more than 1 task from the queue at a time before we know for sure we have the
     * resources to execute it. That way if the queue is backed by a distributed queue, for example, we ensure as many
     * items are available for other workers as possible.
     *
     * There's some duplication between this and getWithBuffer(timeout), but behavior is sufficiently different in the
     * non-blocking case that it seemed clearer to keep them separate.
     *
     * If timeout < 0, it is treated as "no timeout"
     *
     * @param timeout if < 0, it is treated as "no timeout". Otherwise, treated as a best-effort max wait.
     * @return an item if present, or the value returned by the appropriate input function/supplier otherwise
     */
    private T pollWithBuffer(long timeout, TimeUnit unit) throws InterruptedException {
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


    /** must have lock before calling this method! */
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

    /** must have lock before calling this method */
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

    private boolean shouldLock() {
        return strict;
    }

    protected boolean shouldReturn(T nextItem) {

        boolean shouldReturn = constraintStrategy.shouldReturn(nextItem);
        if (!shouldReturn && (taskTracker != null && taskTracker.currentTasks().isEmpty())) {
            if (log.isDebugEnabled()) {
                log.debug("Constraint strategy says we should not return an item, but task tracker says that there is nothing in progress. So returning it anyway.");
            }
            return true;
        } else {
            return shouldReturn;
        }
    }


    /**
     * Retrieves and removes the head of this queue,
     * or returns {@code null} if this queue is empty, or if we do not yet have sufficient resources for the next item.
     *
     * See the concurrency notes in the class-level javadoc for important notes about the accuracy and threadsafety of
     * this method.
     *
     * @return the next value in the queue or null if we cannot currently execute anything.
     */
    @Override
    public T poll() {
        if (shouldBuffer) {
            return getItemWithBuffer(() -> null, (n) -> null);
        } else {
            return getItemWithoutBuffer(Queue::poll, () -> null, (n) -> null);
        }
    }


    /**
     * Retrieves and removes the head of this queue, waiting if necessary
     * until an element becomes available.
     *
     * See the concurrency notes in the class-level javadoc for important notes about the accuracy and threadsafety of
     * this method.
     *
     * Also note that, since this implementation depends on peek(), it is implemented using a periodic poll of the
     * underlying queue, rather than calling queue.take() directly. The poll frequency can be set as a constructor argument.
     * TODO: exponential decay on the poll frequency
     *
     * @return the head of this queue
     * @throws InterruptedException if interrupted while waiting
     */
    @Override
    public T take() throws InterruptedException {
        return poll(-1, TimeUnit.NANOSECONDS);
    }

    T failForTooMayTries(T item) {
        log.error("Could not take item after " + constrainedItemThresholdMS + " attempts:  " + item);
        //take the item from the delegate
        delegate.remove();
        taskAttemptCounter.removeConstrained(item);
        return failTask(item);
    }


    /**
     * If you would like to implement custom logic after an item has failed too many resource checks, override this method.
     * Some options are to call "cancel()" on tasks, register exceptions, requeue, etc.
     *
     * Note that the result of this method will be returned to the original caller.
     * @param item
     * @return
     */
    protected T failTask(T item) {
        throw new InsufficientResourcesException(item);
    }

    /**
     * If we decide we want pluggable behavior here, take a look at LMAX Disruptor's WaitStrategy classes
     */
    private void sleep() throws InterruptedException {
        if (sleeps != null) {
            sleeps.mark();
        }
        Thread.sleep(retryFrequencyMS);
    }

    /**
     * See the concurrency notes in the class-level javadoc for important notes about the accuracy and threadsafety of
     * this method.
     *
     * Also note that, since this implementation depends on peek(), it is implemented using a periodic poll of the
     * underlying queue, rather than calling queue.take() directly. The poll frequency can be set as a constructor argument.
     *
     * @return the head of this queue, or {@code null} if the
     *         specified waiting time elapses before an element which we have the resources for is available
     * @see #poll()
     */
    @Override
    public T poll(long timeout, TimeUnit unit) throws InterruptedException {
        if (shouldBuffer) {
            return pollWithBuffer(timeout, unit);
        } else {
            return pollWithoutBuffer(timeout, unit);
        }
    }


    private T pollWithoutBuffer(long timeout, TimeUnit unit) throws InterruptedException {
        // we have to do a little extra work here because we may have to wait for some time before we have enough resources.
        // We make a reasonable effort to ensure that the combined wait-until-resources-are-available and poll time don't
        // exceed the desired timeout.
        long totalSleepNanos = 0;
        long startNanos = System.nanoTime();
        long timeoutNanos = unit.toNanos(timeout);
        // we treat "timeoutNanos < 0" as "no limit"
        while (timeoutNanos < 0 || totalSleepNanos < timeoutNanos) {
            try {
                return getItemWithoutBuffer(Queue::poll, () -> {
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

    private boolean shouldFail(T nextItem) {
        if (taskAttemptCounter != null) {
            //increment number of tries for this item
            int attempts = taskAttemptCounter.incrementConstrained(nextItem);
            if (attempts >= constrainedItemThresholdMS) {
                if (failAfterAttemptThresholdReached) {
                    return true;
                } else {
                    //just log it and continue to try
                    if (log.isTraceEnabled()) {
                        log.trace("Could not take item after " + (constrainedItemThresholdMS * retryFrequencyMS / 1000.0) + " seconds:" + nextItem);
                    }
                    //set retries back to 1
                    taskAttemptCounter.resetConstrained(nextItem);
                    return false;
                }
            }
        }
        return false;
    }

    /**
     * This method does NOT make any resource constraint checks. It just delegates to the underlying queue.
     *
     * @see java.util.Queue#element()
     */
    @Override
    public T element() {
        return delegate.element();
    }

    /**
     * This method does NOT make any resource constraint checks. It just delegates to the underlying queue.
     *
     * @see java.util.Queue#peek()
     */
    @Override
    public T peek() {
        return delegate.peek();
    }

    /**
     * This method does not make any resource constraint checks. It just delegates to the underlying queue. So it is
     * literally "how many elements in the queue?" not "how many elements do I have resources to execute?"
     *
     * @see java.util.Queue#size()
     */
    @Override
    public int size() {
        return delegate.size();
    }

    /**
     * This method does not make any resource constraint checks. It just delegates to the underlying queue. So it is
     * literally "are there any elements in the queue?" not "do I have resources to execute any elements in the queue?"
     *
     * @see java.util.Queue#isEmpty()
     */
    @Override
    public boolean isEmpty() {
        return delegate.isEmpty();
    }

    /**
     * This method does not make any resource constraint checks. It just delegates to the underlying queue.
     *
     * @see java.util.Queue#contains(Object)
     */
    @Override
    public boolean contains(Object o) {
        return delegate.contains(o);
    }

    /**
     * This method does not make any resource constraint checks. It just delegates to the underlying queue.
     *
     * @see java.util.concurrent.BlockingQueue#drainTo(java.util.Collection)
     */
    public int drainTo(Collection<? super T> c) {
        int count = delegate.drainTo(c);
        if (pendingItems != null) {
            pendingItems.dec(count);
        }
        return count;
    }

    /**
     * This method does not make any resource constraint checks. It just delegates to the underlying queue.
     *
     * @see java.util.concurrent.BlockingQueue#drainTo(java.util.Collection, int)
     */
    public int drainTo(Collection<? super T> c, int maxElements) {
        int count = delegate.drainTo(c, maxElements);
        if (pendingItems != null) {
            pendingItems.dec(count);
        }
        return count;
    }

    /**
     * This method just delegates to the underlying queue; the returned iterator will NOT honor any resource constraints.
     * This may change in the future.
     *
     * @see java.util.concurrent.BlockingQueue#iterator()
     */
    @Override
    public Iterator<T> iterator() {
        return delegate.iterator();
    }

    /**
     * This method does NOT make any resource constraint checks. It just delegates to the underlying queue.
     *
     * @see java.util.Queue#toArray()
     */
    @Override
    public Object[] toArray() {
        return delegate.toArray();
    }

    /**
     * This method does NOT make any resource constraint checks. It just delegates to the underlying queue.
     *
     * @see java.util.Queue#toArray(Object[])
     */
    @Override
    public <T1> T1[] toArray(T1[] a) {
        return delegate.toArray(a);
    }

    /**
     * This method does not make any resource constraint checks. It just delegates to the underlying queue.
     *
     * @see java.util.Queue#remove(Object)
     */
    @Override
    public boolean remove(Object o) {
        if (pendingItems != null) {
            pendingItems.dec();
        }
        return delegate.remove(o);
    }

    /**
     * This method does not make any resource constraint checks. It just delegates to the underlying queue.
     *
     * @see java.util.Queue#containsAll(java.util.Collection)
     */
    @Override
    public boolean containsAll(Collection<?> c) {
        return delegate.containsAll(c);
    }

    /**
     * This method does not make any resource constraint checks. It just delegates to the underlying queue.
     *
     * @see java.util.concurrent.BlockingQueue#addAll(java.util.Collection)
     */
    public boolean addAll(Collection<? extends T> c) {
        if (pendingItems != null) {
            pendingItems.inc(c.size());
        }
        return delegate.addAll(c);
    }

    /**
     * This method does not make any resource constraint checks. It just delegates to the underlying queue.
     *
     * @see java.util.concurrent.BlockingQueue#removeAll(java.util.Collection)
     */
    @Override
    public boolean removeAll(Collection<?> c) {
        if (pendingItems != null) {
            pendingItems.dec(c.size());
        }
        return delegate.removeAll(c);
    }

    /**
     * This method does not make any resource constraint checks. It just delegates to the underlying queue.
     *
     * @see java.util.concurrent.BlockingQueue#retainAll(java.util.Collection)
     */
    @Override
    public boolean retainAll(Collection<?> c) {
        //TODO: pendingItems isn't tracking changes via this method yet.
        return delegate.retainAll(c);
    }

    /**
     * This method does not make any resource constraint checks. It just delegates to the underlying queue.
     *
     * @see java.util.concurrent.BlockingQueue#clear()
     */
    @Override
    public void clear() {
        if (pendingItems != null) {
            int size = delegate.size();
            pendingItems.dec(size);
        }
        delegate.clear();
    }

    /**
     * returns true if o is an instance of ResourceConstrainingQueues and their underlying queues are equal.
     * Most of the definition of "equality", then, is delegated to the underlying queues.
     *
     * @see java.util.Collection#equals(Object)
     */
    @Override
    public boolean equals(Object o) {
        return o instanceof ResourceConstrainingQueue && delegate.equals(o);
    }

    @Override
    public int hashCode() {
        return delegate.hashCode();
    }


    /**
     * Add t to the back of the queue. Note that ResourceConstrainingQueue doesn't make any resource constraint checks
     * on insertions into the queue, only on removals.
     *
     * @param t
     * @throws InterruptedException
     * @see java.util.concurrent.BlockingQueue#put(Object)
     */
    public void put(T t) throws InterruptedException {
        markAddition();
        delegate.put(t);
    }

    /**
     * Note that ResourceConstrainingQueue doesn't make any resource constraint checks
     * on insertions into the queue, only on removals.
     *
     * @param t
     * @throws InterruptedException
     * @see java.util.concurrent.BlockingQueue#offer(Object, long, java.util.concurrent.TimeUnit)
     */
    public boolean offer(T t, long timeout, TimeUnit unit) throws InterruptedException {
        markAddition();
        return delegate.offer(t, timeout, unit);
    }

    @Override
    public int remainingCapacity() {
        return delegate.remainingCapacity();
    }

    /**
     * Get the constraint strategy that we're using to decide whether to hand out items.
     *
     * @return
     */
    public ConstraintStrategy<T> getConstraintStrategy() {
        return constraintStrategy;
    }

    public void registerMetrics(MetricRegistry metrics, String name) {
        metrics.register(name(ResourceConstrainingQueue.class, name, "size"), (Gauge<Integer>) this::size);


        pendingItems = metrics.counter(name(ResourceConstrainingQueue.class, name, "pending-items"));
        trackedRemovals = metrics.meter(name(ResourceConstrainingQueue.class, name, "remove-poll-take"));
        additions = metrics.meter(name(ResourceConstrainingQueue.class, name, "add-offer-put"));
        sleeps = metrics.meter(name(ResourceConstrainingQueue.class, "sleeps"));

        if (this.constraintStrategy instanceof MetricsAware) {
            ((MetricsAware) constraintStrategy).registerMetrics(metrics, name);
        }
        if (this.delegate instanceof MetricsAware) {
            ((MetricsAware) delegate).registerMetrics(metrics, name);
        }
        if (this.taskTracker instanceof MetricsAware) {
            ((MetricsAware) taskTracker).registerMetrics(metrics, name);
        }

    }

    public boolean isFailAfterAttemptThresholdReached() {
        return failAfterAttemptThresholdReached;
    }

    public void setFailAfterAttemptThresholdReached(boolean failAfterAttemptThresholdReached) {
        this.failAfterAttemptThresholdReached = failAfterAttemptThresholdReached;
    }


    public static class ResourceConstrainingQueueBuilder<T> {
        BlockingQueue<T> builderdelegate = null;
        private long builderresourcePollFrequencyMS = DEFAULT_POLL_FREQ_MS;
        ConstraintStrategy<T> builderConstraintStrategy;
        TaskTracker<T> builderTaskTracker;
        boolean useTaskTracker = true;
        boolean builderStrict = true;
        boolean useBuffer = true;
        long noResourcesTimeLimit = DEFAULT_CONSTRAINED_ITEM_THRESHOLD_MS;

        public ResourceConstrainingQueueBuilder<T> withConstraintStrategy(ConstraintStrategy<T> cs) {
            this.builderConstraintStrategy = cs;
            return this;
        }

        public ResourceConstrainingQueueBuilder<T> withTaskTracker(TaskTracker<T> tt) {
            this.builderTaskTracker = tt;
            return this;
        }

        public ResourceConstrainingQueueBuilder<T> useTaskTracker(boolean useTaskTracker) {
            this.useTaskTracker = useTaskTracker;
            return this;
        }

        public ResourceConstrainingQueueBuilder<T> withBlockingQueue(BlockingQueue<T> q) {
            this.builderdelegate = q;
            return this;
        }

        public ResourceConstrainingQueueBuilder<T> useBuffer(boolean buffer) {
            this.useBuffer = buffer;
            return this;
        }

        public ResourceConstrainingQueueBuilder<T> withRetryFrequency(long pollFrequencyInMS) {
            this.builderresourcePollFrequencyMS = pollFrequencyInMS;
            return this;
        }

        public ResourceConstrainingQueueBuilder<T> strict(boolean strict) {
            this.builderStrict = strict;
            return this;
        }

        /**
         * The maximum amount of time to wait until there are sufficient resources for an item.
         * @param timeLimit
         * @param timeUnit
         * @return
         */
        public ResourceConstrainingQueueBuilder<T> withConstrainedItemTimeLimit(long timeLimit, TimeUnit timeUnit) {
            this.noResourcesTimeLimit = timeUnit.toMillis(timeLimit);
            return this;
        }

        public ResourceConstrainingQueue<T> build() {
            BlockingQueue<T> d = builderdelegate;
            long pollfreq = builderresourcePollFrequencyMS;
            ConstraintStrategy<T> cs = builderConstraintStrategy;
            if (cs == null) {
                cs = ConstraintStrategies.defaultReactiveConstraintStrategy(pollfreq);
            }
            if (d == null) {
                d = new LinkedBlockingQueue<T>();
            }
            if (useTaskTracker && builderTaskTracker == null) {
                builderTaskTracker = TaskTrackers.defaultTaskTracker();
            }
            return new ResourceConstrainingQueue<T>(d, cs, pollfreq, builderStrict, useBuffer, builderTaskTracker, noResourcesTimeLimit);
        }

    }


    protected static class TaskAttemptCounter {

        final ConcurrentHashMap<Object, Integer> unableToExecuteTaskTries = new ConcurrentHashMap<Object, Integer>();

        public int incrementConstrained(Object item) {
            Integer attempts = unableToExecuteTaskTries.get(item);
            if (attempts == null) {
                attempts = 0;
            }
            attempts = attempts + 1;
            unableToExecuteTaskTries.put(item, attempts);
            return attempts;
        }

        public void resetConstrained(Object item) {
            if (unableToExecuteTaskTries.contains(item)) {
                unableToExecuteTaskTries.put(item, 1);
            }
        }

        public void removeConstrained(Object item) {
            unableToExecuteTaskTries.remove(item);
        }

    }

    public static class InsufficientResourcesException extends NoSuchElementException {
        public InsufficientResourcesException(Object o) {
            super("Insufficient resources to execute "+ o.toString());
        }

        // sometimes you feel like an arg... sometimes you don't.
        public InsufficientResourcesException() { }
    }
}
