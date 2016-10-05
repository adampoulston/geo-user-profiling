import os
from csv import DictReader
from collections import defaultdict
from json import loads, dumps
from home_location_assigner import HomeLocationAssigner
from lookup_boundary import BoundaryLookup
import traceback
    
def label_profiles(profiles_directory, oa_sf_dir, lad_sf_dir):  
    # Load home location assigner to get coordinates from geolocated tweets
    hla = HomeLocationAssigner()

    users_dir = profiles_directory

    # Create directory to store results if needed
    if not os.path.exists("output_datasets"):
        os.makedirs("output_datasets")

    # load in shapefiles for OAs and LADs

    oa_shp_loc = None
    if not oa_sf_dir.endswith("/"):
        oa_sf_dir = oa_sf_dir + "/"
    for fname in os.listdir(oa_sf_dir):
        if fname.endswith(".shp"):
            oa_shp_loc = oa_sf_dir+fname.strip(".shp")
    oa_lookup = BoundaryLookup(oa_shp_loc)


    lad_shp_loc = None
    if not lad_sf_dir.endswith("/"):
        lad_sf_dir = lad_sf_dir + "/"
    for fname in os.listdir(lad_sf_dir):
        if fname.endswith(".shp"):
            lad_shp_loc = lad_sf_dir+fname.strip(".shp")
    lad_lookup = BoundaryLookup(lad_shp_loc)

    # load mapping between output areas and output area classifications
    oa_oac_map = defaultdict(lambda: None)
    with open("region_labels/oac.csv") as f:
        r = DictReader(f)
        for row in r:
            oa_oac_map[row['Output Area Code']] = row['Supergroup Code']

    #load mapping between LADS and LADC
    lad_ladc_map = defaultdict(lambda: None)
    with open("region_labels/ladc.csv") as f:
        r = DictReader(f)
        for row in r:
            lad_ladc_map[row['Code']] = row['Supergroup']

    # Create file handles to store results
    oa_output = open("output_datasets/uk_tweets_40_oac_p","w+", 0)
    lad_output = open("output_datasets/uk_tweets_40_ladc_p","w+", 0)

    # iterate through provided profiels
    for filename in os.listdir(users_dir):
        oac = None
        ladc = None

        #Loads the raw user object
        with open(users_dir + filename) as f:
            tweets = [loads(line.strip()) for line in f]
            #count geolocated tweets
            total_geolocated = len([None for t in tweets if t['coordinates']])
        try:
            # only consider profiles with 10 or more tweets
            if total_geolocated > 9:
                # assign the home location
                home = hla.assign_home(tweets)
                # ignore the profile if is suspicious, or a fine grained home location judgement could not be made
                if home['suspicion_points'] == 0 and home['hc-med-density'] < 0.5:
                    #lookup OA
                    oa = oa_lookup.lookup_boundary_ordered(*home['hc'][::-1])
                    if oa:
                        #if found, take the OAC
                        oac = oa_oac_map[oa[0]]
                    #lookup LAD
                    lad = lad_lookup.lookup_boundary_ordered(*home['hc'][::-1])
                    if lad:
                        #if found, take the LADC
                        ladc = lad_ladc_map[lad[0]]
                    #write the labeled profiles to file
                    if oac or ladc:
                        home['tweets'] = [t['text'] for t in tweets]
                        
                        #don't store the location at coordinate level for privacy's sake
                        del home['hc']

                        if oac:
                            home['label'] = oac
                            oa_output.write(dumps(home)+"\n")
                        if ladc:
                            home['label'] = ladc
                            lad_output.write(dumps(home)+"\n")   
        except Exception, e:
            traceback.print_exc()

    oa_output.close()
    lad_output.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description = "Script to label a directory of twitter profiles with demographics.")
    parser.add_argument('--profiles', type=str, required = True, help = "Directory of raw Twitter user object in JSON format.")
    parser.add_argument('--oasfdir', type=str, required = True, help = "Directory containg output area shapefile.")
    parser.add_argument('--ladsfdir', type=str, required = True, help = "Directory containg local authority district shapefile.")

    arguments = parser.parse_args()

    label_profiles(arguments['profiles'], arguments['oasfdir'], arguments['ladsfdir'])
