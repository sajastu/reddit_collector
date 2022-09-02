# TLDR9+ and TLDRHQ

This resource contains the resources introduced in [_TLDR9+: A Large Scale Resource for Extreme Summarization of Social Media Posts_](https://aclanthology.org/2021.newsum-1.15/), including two Reddit-based TLDR summarization datasets. For more information about the construction stages of these datasets, please refer to the main paper. 

## Dataset links
You may download the introduced datasets from Google Drive links below:

| **Dataset** | # instances | **Link**                                                                                       |
|-------------|-------------|------------------------------------------------------------------------------------------------|
| TLDR9+      |  9,227,437           | [Download](https://drive.google.com/file/d/1hYJqH-czgbw78rvxajzj56tDLr6lkdjh/view?usp=sharing) |
| TLDRHQ      | 1,671,099           | [Download](https://drive.google.com/file/d/1jCi0Mn0k-pid5SSTafov11-e1A9LEZed/view?usp=sharing) |

After downloading the datasets from links above, un-tar the compressed file by `tar -xvf {DATASET_FILE}.tar`. You will then find different data splits (`datasets-m*/`) in the root directory, each with 25001 instances.

## Dataset structure
Each instance of TLDR9+ and TLDRHQ in the dataset has the following attributes:

````
{
    "id": ID of the reddit post,
    "document": User's post text,
    "summary": Summary/TLDR of the written post,
    "ext_labels": Extractive labels of the post's sentences.
    "rg_labels": Rouge scores of the post's sentences.
}

````
Notes:
* `id` is not the actual ID of the post in the Reddit discussion forum, but it's rather a generated ID that follows a specific format. For instance, `*RS*` (`*RC*`) format shows that the instance is of type Submission (Comment). 
* `document` is split by the sentences; hence, you will find `</s><s>` tokens within the document's text, indicating the sentence boundaries.


## Reddit Miner
We release the source implementation to collect the TL;DR instances (i.e., user_post and TL;DR summary pairs) from Pushshift data repository. 

### How to run 
First, install the required package by running the following:

````
pip install -r requirements.txt
````
As the Pushshift files are quite large, we use the [GNU Parallel](https://www.gnu.org/software/parallel/) shell tool for parallel processing. Hence, you will need install this shell tool. 

For quick start, simply run: `bash dl_uncompress.sh`. The `dl_uncompress.sh` script calls the utilities to download, uncompress, and mine the instances to create the
TL;DR data collection.

Some notes about the implementation are outlined below:

- `fetch_file_links.py` gathers the link of each compressed file (either comments/submissions that should be specified by the user) specified within a timeframe. It specifically takes in the following arguments:
  * `-scrap`: choose between _comments_ or _submissions_. Default is set to _comments_.
  * `-start_date`: the starting date in the format of `YYYY-MM` (e.g. 2019-04).
  * `-end_date`: the ending date in the format of `YYYY-MM` (e.g. 2020-06).


- `main.py` is the main class that calls different utilities to filter the Reddit posts to those with TL;DRs. Arguments are listed below:
  - `-mode`: specifies the mode of the processing. It can be either _"preprocess"_ or _"comment_agg"_
  - `-read_dir`: the input directory, where all uncompressed files Reddit files are located in.  
  - `-write_dir`: the output directory to write the filtered instances one-by-one.
  - `-tldr_th`: word threshold for filtering TL;DRs. The instances that do not pass this threshold will be dropped.
  - `-lower`: a flag indicating either cased or uncased instances should be mined.

## Citation

If you intend to use these datasets, please cite the following research paper: 

````
@inproceedings{Sotudeh2021Tldr9,
    title = "TLDR9+: A Large Scale Resource for Extreme Summarization of Social Media Posts",
    author = "Sotudeh, Sajad  and
      Deilamsalehy, Hanieh  and
      Dernoncourt, Franck  and
      Goharian, Nazli",
    booktitle = "Proceedings of the Third Workshop on New Frontiers in Summarization",
    month = nov,
    year = "2021",
    publisher = "Association for Computational Linguistics",
    url = "https://aclanthology.org/2021.newsum-1.15",
    pages = "142--151",
}
````

## Contact
Please contact Sajad Sotudeh ( `{firstname}@ir.cs.gerogetown.edu` ) in case you have any question(s).