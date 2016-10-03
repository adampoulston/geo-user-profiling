import os
from csv import DictReader
from collections import defaultdict
from json import loads, dumps
from home_location_assigner import HomeLocationAssigner
from lookup_boundary import BoundaryLookup

hla = HomeLocationAssigner()

data_dir = "raw_data/"
users_dir = data_dir+"profiles/"

if not os.path.exists("output_datasets"):
    os.makedirs("output_datasets")

oa_shp_loc = "shapefiles/oa_boundaries/OA_2011_EW_BFE_V2"
oa_lookup = BoundaryLookup(oa_shp_loc)

lad_shp_loc = "shapefiles/lad_boundaries/LAD_DEC_2011_GB_BFE"
lad_lookup = BoundaryLookup(lad_shp_loc)

oa_oac_map = defaultdict(lambda: None)
with open("region_labels/oac.csv") as f:
    r = DictReader(f)
    for row in r:
        oa_oac_map[row['Output Area Code']] = row

lad_ladc_map = defaultdict(lambda: None)
with open("region_labels/ladc.csv") as f:
    r = DictReader(f)
    for row in r:
        lad_ladc_map[row['Code']] = row

oa_output = open("output_datasets/oac_p","w+", 0)
lad_output = open("output_datasets/ladc_p","w+", 0)

for filename in os.listdir(users_dir):
    oac = None
    ladc = None

    with open(users_dir + filename) as f:
        tweets = [loads(line.strip()) for line in f]
    try:
        home = hla.assign_home(tweets)
        oa = oa_lookup.lookup_boundary_ordered(*home['hc'][::-1])
        if oa:
            oac = oa_oac_map[oa[0]]
        lad = lad_lookup.lookup_boundary_ordered(*home['hc'][::-1])
        if lad:
            ladc = lad_ladc_map[lad[0]]
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
        print str(e)

oa_output.close()
lad_output.close()
     
       

