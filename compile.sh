set -xe
OUTPUT_FILE=dist.csv

zcat ngap_distribution_2018_02.csv.gz | head -1 > $OUTPUT_FILE
for file in ngap_distribution*.csv.gz; do
    zcat $file | tail -n +2 >> $OUTPUT_FILE
done

gzip -f $OUTPUT_FILE
