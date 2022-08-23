# TLDR9+ and TLDRHQ

This resource contains the resources introduced in [_TLDR9+: A Large Scale Resource for Extreme Summarization of Social Media Posts_](https://aclanthology.org/2021.newsum-1.15/), including two Reddit-based TLDR summarization datasets. For more information about the construction stages of these datasets, please refer to the main paper. 

## Dataset links
You may download the introduced datasets from Google Drive links below:

| **Dataset** | # instances | **Link**                                                                                       |
|-------------|-------------|------------------------------------------------------------------------------------------------|
| TLDR9+      |  9,227,437           | [Download](https://drive.google.com/file/d/1hYJqH-czgbw78rvxajzj56tDLr6lkdjh/view?usp=sharing) |
| TLDRHQ      | 1,671,099           | [Download](https://drive.google.com/file/d/1jCi0Mn0k-pid5SSTafov11-e1A9LEZed/view?usp=sharing) |

After downloading the datasets from links above, extract the dataset in the compressed file by `tar -xvf {DATASET_FILE}.tar`. You will then find different data splits (`datasets-m*/`) in the root directory, each with 25001 instances.

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
* `id` is not the actual ID of the post in the Reddit discussion forum, but it's rather a  automatically generated ID.
* `document` is split by the sentences; hence, you will find `</s><s>` tokens within the document's text, that indicates the sentence boundaries.


## Reddit miner
The code will get updated soon. 

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