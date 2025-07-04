#!/bin/sh

exec /bin/streaming \
  -port $PORT \
  -segment-size $SEGMENT_SIZE \
  -systems-dir "$SYSTEMS_DIR" \
  -models-dir "$MODELS_DIR" \
  -queue-dir "$QUEUE_DIR"
