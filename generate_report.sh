set -xe
YEAR=$(date --date "1 day ago" +"%Y")
MONTH=$(date --date "1 day ago" +"%m")
grep -hE '(gsfc-ngap-p-|asf-ngap2-p-).* REST\.GET\.OBJECT .*"GET /S1.*\.zip.*userid=.*" (200|206) ' /storage/syslog/awss3access/$YEAR/$MONTH/accesslog | cut -c133-9999 > log.txt
. venv/bin/activate
python parse.py log.txt ngap_distribution_${YEAR}_${MONTH}.csv
gzip -f ngap_distribution_${YEAR}_${MONTH}.csv
rm log.txt
sh compile.sh
