
python untied_all_comments.py > url.list
readarray a < url.list


for m in "${a[@]}"
do
    mv $m /mnt/ilcompf0d0/user/dernonco/corpora/reddit/original/comments/missed_comments/
    echo $m
done


#cat url.list -print0 | xargs -0 -I {} -P 10 mv {} /mnt/ilcompf0d0/user/dernonco/corpora/reddit/original/comments/missed_comments/
