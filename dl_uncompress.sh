

# GLOBAL arguments. Pls change this based off your configs.

SCRAP="comments" # choose between `comments` or `submissions`
#FILE_READ_PATH="/path/to/downloaded/files"
#FILE_WRITE_PATH="/path/to/written/files"
FILE_READ_PATH="/disk0/shabnam/.cache/reddit/downloads/"
FILE_WRITE_PATH="/disk0/shabnam/.cache/reddit/data"

##################################

COLLECTOR_PATH=$(pwd)
echo ""
echo "Root path is '$COLLECTOR_PATH'"
mkdir -p $FILE_READ_PATH

## check the date range arguments if you want to scrap specific period files
#python fetch_file_links.py -scrap $SCRAP -start_date "2022-06" -end_date "2022-07" > url.list
##readarray a < url.list
#
#
#mv url.list $FILE_READ_PATH
#cd $FILE_READ_PATH
#
#echo "Starting to download ..."
#echo "This may take several minutes..."
#
#cat url.list | parallel -j10 wget {}
#
## remove the url list
#rm url.list
#
#echo "Un-compressing downloaded files..."
## unzip
#find . -name '*.zst' -print0 | xargs -0 -I {} -P 15 unzstd --long=31 {} > {}.json
#find . -name '*.bz2' -print0 | xargs -0 -I {} -P 10 bzip2 -v -d {}
#find . -name '*.xz' -print0 | xargs -0 -I {} -P 5 unxz {}
#
## At this stage, you can remove the compressed files to free up some disk memory!
## It's up tp you, if you want to keep the compressed files, then uncomment the following 3 lines
#rm *.zst
#rm *.bz2
#rm *.xz

# return to the collector root
cd $COLLECTOR_PATH

python main.py  -mode preprocess \
                -read_dir $FILE_READ_PATH \
                -write_dir $FILE_WRITE_PATH \
                -tldr_th 4 \


