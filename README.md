# User profiling with geo-located posts and demographic data
This repo contains the data collection, profile labelling and classification scripts used to generate the results in the paper:
>User profiling with geo-located posts and demographic data

>Adam Poulston, Mark Stevenson and Kalina Bontcheva

>NLP+CSS at EMNLP 2016 (To appear)

##Setup
###Python
The code here has been tested in `python 2.7.12` on Linux. Use of a tool such as [`pyenv`](https://github.com/yyuu/pyenv) (with [`virtualenv`](https://github.com/yyuu/pyenv-virtualenv)) to manage `python` versions is suggested. 

Required packages listed in `requirements.txt` can be installed through `pip` (`pip install -r requirements.txt`). Packages requiring a little more work are listed in `additional_requirements.txt`.

###Other 
To label profiles with their output areas and local authorities, two shapefiles are required, please refer to the README in `shapefiles` for more info.

To use the data collection scripts place your [Twitter keys](https://dev.twitter.com/) in the file named `keys`.

##Usage
###Data collection
To build a collection of candidate UK profiles run `stream_twitter.py` for a while, this will store hourly blocks of raw tweets in JSON format, as well as populating a list of user ids to download. Once the candidate list has some entries, start the profile collection script (`download_profiles.py`), this will populate a file for each user with all of their raw tweets.

Run data collection until you are satisfied with the number of profiles in your dataset. The results in the paper are based on 2000 profiles per label (16000 total) for each set of labels(OAC and LAC).

###Data labelling
To label the profiles with LAC and OAC, run `label_profiles.py` with the directory containing the gathered profiles as input, e.g.:
```
python label_profiles.py --profiles raw_data/profiles/ --oasfdir shapefiles/oa_shapefile_dir/ --ladsfdir shapefiles/lad_shapefile_dir/ 
```
a directory (`output_datasets`) will be created and populated with the two resulting datasets (OAC-P and LAC-P).

###Classification pipeline
After building the datasets, run the pipeline using
```
python classification_pipeline.py --inputfile dataset
```
accuracy will be reported, although other metrics from `sklearn` would be easy to add.
