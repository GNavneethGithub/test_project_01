#!/usr/bin/env bash

SESSION="elasticdump_parallel"
LOGDIR="./elasticdump_logs"
mkdir -p "$LOGDIR"

###############################
# MASKED VALUES — YOU FILL IN
###############################
AUTH_HEADER='{"Authorization": "Basic ****"}'
S3_AK="****"
S3_SK="****"

INPUT_CA="/tmp/CCCCC/compnay_name-ca.pem"
ES_INPUT="https://eeeeeee.011y.compnay_name.com:9200/index_01*"

BASE_S3_PATH="s3://xx/yy/zz/gg/2025-11-24"

###############################
# BUILD 5 INTERVAL COMMANDS
###############################

CMD1="elasticdump \
--input=\"$ES_INPUT\" \
--output=\"$BASE_S3_PATH/09-00/index_01_0900_0905.json\" \
--use-ssl=true --tlsAuth \
--headers='$AUTH_HEADER' \
--input-ca=\"$INPUT_CA\" \
--type=data \
--searchBody='{\"query\":{\"bool\":{\"must\":[{\"range\":{\"datetime\":{\"gte\":\"2025-11-24T09:00:00Z\",\"lt\":\"2025-11-24T09:05:00Z\",\"time_zone\":\"PST8PDT\"}}}]}}}' \
--s3AccessKeyId=$S3_AK \
--s3SecretAccessKey=$S3_SK \
--retryAttempts=3 --retryDelay=60000 \
--limit=5000 --fileSize=100mb --timeout=180000 | tee \"$LOGDIR/cmd1.log\""

CMD2="elasticdump \
--input=\"$ES_INPUT\" \
--output=\"$BASE_S3_PATH/09-05/index_01_0905_0910.json\" \
--use-ssl=true --tlsAuth \
--headers='$AUTH_HEADER' \
--input-ca=\"$INPUT_CA\" \
--type=data \
--searchBody='{\"query\":{\"bool\":{\"must\":[{\"range\":{\"datetime\":{\"gte\":\"2025-11-24T09:05:00Z\",\"lt\":\"2025-11-24T09:10:00Z\",\"time_zone\":\"PST8PDT\"}}}]}}}' \
--s3AccessKeyId=$S3_AK \
--s3SecretAccessKey=$S3_SK \
--retryAttempts=3 --retryDelay=60000 \
--limit=5000 --fileSize=100mb --timeout=180000 | tee \"$LOGDIR/cmd2.log\""

CMD3="elasticdump \
--input=\"$ES_INPUT\" \
--output=\"$BASE_S3_PATH/09-10/index_01_0910_0915.json\" \
--use-ssl=true --tlsAuth \
--headers='$AUTH_HEADER' \
--input-ca=\"$INPUT_CA\" \
--type=data \
--searchBody='{\"query\":{\"bool\":{\"must\":[{\"range\":{\"datetime\":{\"gte\":\"2025-11-24T09:10:00Z\",\"lt\":\"2025-11-24T09:15:00Z\",\"time_zone\":\"PST8PDT\"}}}]}}}' \
--s3AccessKeyId=$S3_AK \
--s3SecretAccessKey=$S3_SK \
--retryAttempts=3 --retryDelay=60000 \
--limit=5000 --fileSize=100mb --timeout=180000 | tee \"$LOGDIR/cmd3.log\""

CMD4="elasticdump \
--input=\"$ES_INPUT\" \
--output=\"$BASE_S3_PATH/09-15/index_01_0915_0920.json\" \
--use-ssl=true --tlsAuth \
--headers='$AUTH_HEADER' \
--input-ca=\"$INPUT_CA\" \
--type=data \
--searchBody='{\"query\":{\"bool\":{\"must\":[{\"range\":{\"datetime\":{\"gte\":\"2025-11-24T09:15:00Z\",\"lt\":\"2025-11-24T09:20:00Z\",\"time_zone\":\"PST8PDT\"}}}]}}}' \
--s3AccessKeyId=$S3_AK \
--s3SecretAccessKey=$S3_SK \
--retryAttempts=3 --retryDelay=60000 \
--limit=5000 --fileSize=100mb --timeout=180000 | tee \"$LOGDIR/cmd4.log\""

CMD5="elasticdump \
--input=\"$ES_INPUT\" \
--output=\"$BASE_S3_PATH/09-20/index_01_0920_0925.json\" \
--use-ssl=true --tlsAuth \
--headers='$AUTH_HEADER' \
--input-ca=\"$INPUT_CA\" \
--type=data \
--searchBody='{\"query\":{\"bool\":{\"must\":[{\"range\":{\"datetime\":{\"gte\":\"2025-11-24T09:20:00Z\",\"lt\":\"2025-11-24T09:25:00Z\",\"time_zone\":\"PST8PDT\"}}}]}}}' \
--s3AccessKeyId=$S3_AK \
--s3SecretAccessKey=$S3_SK \
--retryAttempts=3 --retryDelay=60000 \
--limit=5000 --fileSize=100mb --timeout=180000 | tee \"$LOGDIR/cmd5.log\""

###############################
# START TMUX WITH 5 PANES
###############################

tmux new-session -d -s "$SESSION" "$CMD1"
tmux split-window -v -t "$SESSION" "$CMD2"
tmux split-window -v -t "$SESSION" "$CMD3"
tmux split-window -v -t "$SESSION" "$CMD4"
tmux split-window -v -t "$SESSION" "$CMD5"

tmux select-layout -t "$SESSION" tiled
tmux attach -t "$SESSION"
