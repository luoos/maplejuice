LOG_DIR="sample_logs"
LOG_FILE_NAME="sample.log"
LOG_LINK="https://raw.githubusercontent.com/ocatak/apache-http-logs/master/acunetix.txt"

# Create sample logs folder if not exists
if [ ! -d $LOG_DIR ]; then
    mkdir $LOG_DIR
    echo "Create folder: sample_logs"
fi

pushd $LOG_DIR

if [ ! -f $LOG_FILE_NAME ]; then
    # Download sample log file
    curl -o $LOG_FILE_NAME $LOG_LINK
fi

popd
