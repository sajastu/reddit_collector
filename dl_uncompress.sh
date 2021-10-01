#
#
python check_file_list.py > url.list
readarray a < url.list

#cp url.list /mnt/ilcompf0d0/user/dernonco/corpora/reddit/original/submissions/0-rem/
#cd /mnt/ilcompf0d0/user/dernonco/corpora/reddit/original/submissions/0-rem/

cp url.list /mnt/ilcompf0d0/user/dernonco/corpora/reddit/original/comments/missed_comments/
cd /mnt/ilcompf0d0/user/dernonco/corpora/reddit/original/comments/missed_comments/

echo "Starting to download ..."
cat url.list | parallel -j10 wget {}


rm url.list



#rm /mnt/ilcompf0d0/user/dernonco/corpora/reddit/original/comments/missed_comments/*2020-01.zst
#rm /mnt/ilcompf0d0/user/dernonco/corpora/reddit/original/comments/missed_comments/*2020-02.zst
#rm /mnt/ilcompf0d0/user/dernonco/corpora/reddit/original/comments/missed_comments/*2020-03.zst
#rm /mnt/ilcompf0d0/user/dernonco/corpora/reddit/original/comments/missed_comments/*2020-04.zst
#rm /mnt/ilcompf0d0/user/dernonco/corpora/reddit/original/comments/missed_comments/*2020-05.zst
#rm /mnt/ilcompf0d0/user/dernonco/corpora/reddit/original/comments/missed_comments/*2020-06.zst
#rm /mnt/ilcompf0d0/user/dernonco/corpora/reddit/original/comments/missed_comments/*2020-07.zst
#rm /mnt/ilcompf0d0/user/dernonco/corpora/reddit/original/comments/missed_comments/*2020-08.zst
#rm /mnt/ilcompf0d0/user/dernonco/corpora/reddit/original/comments/missed_comments/*2020-09.zst
#rm /mnt/ilcompf0d0/user/dernonco/corpora/reddit/original/comments/missed_comments/*2020-10.zst
#rm /mnt/ilcompf0d0/user/dernonco/corpora/reddit/original/comments/missed_comments/*2020-11.zst
#rm /mnt/ilcompf0d0/user/dernonco/corpora/reddit/original/comments/missed_comments/*2020-12.zst