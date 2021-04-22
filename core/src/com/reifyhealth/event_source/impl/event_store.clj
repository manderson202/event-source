(ns com.reifyhealth.event-source.impl.event-store)

(defprotocol EventStore
  (initial-version [this]
    "Returns the starting version for a new event stream. This is the expected
    version for a brand new stream that has received no events.")

  (append-to-stream [this stream-id txn-id expected-version events]
    "Appends a new event to the event stream represented by `stream-id`. The
    `txn-id` is used to reject retries that have already been processed. The
    `expected-version` is used for optimistic concurrency control and will
    cause a concurrent modification exception if mismatched with actual
    current version. Finally, a sequence of events to append to the stream
    should be provided.")

  (read-stream
    [this stream-id]
    [this stream-id start-version]
    [this stream-id start-version limit]
    "Reads the event stream represented by `stream-id` from the beginning. Can
    optionally start the read from `start-version` and limit the number of
    results returned with `limit`.")

  (subscribe
    [this subscriber-name event-handler]
    [this subscriber-name event-handler options]
    "Subscribes an event-handler to receive all events in the system with the
    given `subscriber-name`. Can optionally provide an options map with the
    following keys:
      - :start-from : [:origin|:latest], default :origin. Determines where
                      in the stream to start on initial subscription.
      - :stream-id  : by default, subscriptions are for all events in the
                      system. Optionally, a stream-id can be specified to
                      only subscribe to events from this stream.")

  (save-snapshot [this stream-id snapshot]
    "Saves a snapshot of the stream with the given `stream-id`")

  (get-snapshot [this stream-id]
    "Returns the snapshot, if any, for the given stream-id"))
