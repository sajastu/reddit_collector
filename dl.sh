

files=( "https://files.pushshift.io/reddit/submissions/RC_2019-07.zst" "https://files.pushshift.io/reddit/submissions/RC_2019-08.zst" "https://files.pushshift.io/reddit/comments/RC_2019-09.zst" "https://files.pushshift.io/reddit/comments/RC_2019-10.zst" "https://files.pushshift.io/reddit/comments/RC_2019-11.zst" "https://files.pushshift.io/reddit/comments/RC_2019-12.zst" )

for i in "${files[@]}"
do
   wget $i
done






DAYS=( "01" "02" "03" "04" "05" "06" "07" "08" "09" "10" "11" "12" "13" "14" "15" "16" "17" "18" "19" "20" "21" "22" "23" "24" "25" "26" "27" "28" "29" "30")
MONTHS=( "01" "02" "03" "04" "05" "06" )

for m in "${MONTHS[@]}"
do
    for d in "${DAYS[@]}"
    do
        foo="https://files.pushshift.io/reddit/comments/RC_2020-${m}-${d}.zst"
        wget $foo
    done
done



# unzip
find . -name '*.zst' -print0 | xargs -0 -I {} -P 15 unzstd --long=31 {} > {}.json
find . -name '*.bz2' -print0 | xargs -0 -I {} -P 10 bzip2 -v -d {}
find . -name '*.xz' -print0 | xargs -0 -I {} -P 5 unxz {}

tar -jf RS_2011-10.bz2
bzip2 -dc RS_2011-10.bz2 > RS_2011-10.txt