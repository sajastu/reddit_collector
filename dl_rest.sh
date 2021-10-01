
files=( "https://files.pushshift.io/reddit/comments/RC_2020-05.zst" "https://files.pushshift.io/reddit/comments/RC_2020-06.zst" "https://files.pushshift.io/reddit/comments/RC_2020-07.zst" "https://files.pushshift.io/reddit/comments/RC_2020-08.zst" "https://files.pushshift.io/reddit/comments/RC_2020-09.zst" "https://files.pushshift.io/reddit/comments/RC_2020-10.zst" "https://files.pushshift.io/reddit/comments/RC_2020-11.zst" "https://files.pushshift.io/reddit/comments/RC_2020-12.zst" "https://files.pushshift.io/reddit/comments/RC_2021-01.zst" "https://files.pushshift.io/reddit/comments/RC_2021-02.zst" "https://files.pushshift.io/reddit/comments/RC_2021-03.zst" "https://files.pushshift.io/reddit/comments/RC_2021-04.zst" "https://files.pushshift.io/reddit/comments/RC_2021-05.zst" "https://files.pushshift.io/reddit/comments/RC_2021-06.zst" )

printf "%s\n" "${files[@]}" >  url.list

echo "Starting to download ..."
cat url.list | parallel -j10 wget {}
rm url.list


echo "Now un-compressing"
find . -name '*.zst' -print0 | xargs -0 -I {} -P 15 unzstd --long=31 {} > {}.json

rm {}.json
rm *.zst
rm dl_rest.sh

cd /mnt/ilcompfad1/user/dernonco/backup-interns/2021/sajad/reddit_collector/
python main.py -mode preprocess -read_dir /mnt/ilcompf0d0/user/dernonco/corpora/reddit/original/comments2021/ -write_dir /mnt/ilcompn0d1/user/sotudehg0/tldr_comments_2021/ -comments_file /mnt/ilcompfad1/user/dernonco/backup-interns/2021/sajad/tldr_post_comments/mapping_posts_comments.pk

mkdir -p /mnt/ilcompn0d1/user/sotudehg0/tldr_comments_2021_highlights/
mkdir -p /mnt/ilcompn0d1/user/sotudehg0/tldr_comments_2021_tokenized/
mkdir -p /mnt/ilcompn0d1/user/sotudehg0/tldr_comments_2021_tokenized_extended/

cd  /mnt/ilcompfad1/user/dernonco/backup-interns/2021/sajad/RedSumm2021/
python src/preprocess.py -mode tokenize -raw_path  /mnt/ilcompn0d1/user/sotudehg0/tldr_comments_2021_highlights/ -prev_tokenized  /mnt/ilcompn0d1/user/sotudehg0/tldr_comments_2021_tokenized/ -save_path /mnt/ilcompn0d1/user/sotudehg0/tldr_comments_2021_tokenized/

cd /mnt/ilcompfad1/user/dernonco/backup-interns/2021/sajad/reddit_collector/
python prepare_data.py

mkdir -p /mnt/ilcompn0d1/user/sotudehg0/tldr_comments_2021_tokenized_extended_split/
python add_rg_to_ds.py

tar -cf tldr_comments_2021_tokenized_extended_split.tar tldr_comments_2021_tokenized_extended_split/

gupload tldr_comments_2021_tokenized_extended_split.tar