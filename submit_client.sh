spark-submit \
    --master yarn \
    --deploy-mode client \
    Assignment.py \
    --output $1
