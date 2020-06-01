set -xe
OUTPUT_FILE=dist.csv

cat 20200501.csv | head -1 > $OUTPUT_FILE
for file in 202005*.csv; do
    cat $file | tail -n +2 >> $OUTPUT_FILE
done

gzip -f $OUTPUT_FILE
