import numpy as np
import shapefile as sfl
from shapely.geometry import Polygon, box, Point, LineString, LinearRing
from osgeo.osr import SpatialReference, CoordinateTransformation
from scipy.spatial.distance import cdist

class BoundaryLookup(object):

    def __init__(self, shapefile_path):

        #Load resources for coordinate transformations
        epsg27700 = SpatialReference()
        epsg27700.ImportFromEPSG(27700)

        epsg4326 = SpatialReference()
        epsg4326.ImportFromEPSG(4326)

        self.bng2latlon = CoordinateTransformation(epsg27700,epsg4326)
        self.latlon2bng = CoordinateTransformation(epsg4326,epsg27700)

        #Load shapefile
        r = sfl.Reader(shapefile_path)
        shapes = r.shapes()
        #calculate representive coordines for eah point by averaging the bounding box corners
        bboxes = [s.bbox for s in shapes]
        x_coords = [b[0] for b in bboxes] + [b[2] for b in bboxes]
        y_coords = [b[1] for b in bboxes] + [b[3] for b in bboxes]
        self.high_x = np.max(x_coords)
        self.high_y = np.max(y_coords)

        self.low_x = np.min(x_coords)
        self.low_y = np.min(y_coords)

        # print "Upper boundary:",self.high_x, self.high_y
        # print "Lower boundary:", self.low_x, self.low_y

        self.rep_coords = [((b[0]+b[2])/2.0, (b[1]+b[3])/2.0) for b in bboxes]
        self.records = r.records()
        self.shapely_shapes = [Polygon(shape.points) for shape in shapes]

    def check_point(self, x, y):
        return (self.low_x < x < self.high_x) and (self.low_y < y < self.high_y)

    def which_area(self, x, y):
        idx = 0
        p = Point(float(x), float(y))
        for s in self.shapely_shapes:
            if s.contains(p):
                return idx
            idx += 1
        return None

    def which_area_ordered(self, x, y):
        p = Point(float(x), float(y))
        order = self.order_search((p.x, p.y))
        for i in order:
            if self.shapely_shapes[i].contains(p):
                return i
        return None

    def order_search(self, point):
        return np.argsort([dist for sublist in cdist(self.rep_coords,[point]) for dist in sublist])

    def lat_lon_to_bng(self, lat,lon):
        ''' transform a latitude-longitude coordinate to BNG format '''
        try:
            res = self.latlon2bng.TransformPoint(lon,lat)
        except:
           print "Error in lat_lon_to_bng."
           return None
        return (res[0], res[1])

    def lookup_boundary(self, lat, lon):
        bng = self.lat_lon_to_bng(float(lat), float(lon))
        if bng:
            if self.check_point(*bng):
                result_idx = self.which_area(bng[0], bng[1])
                if result_idx:    
                    record = self.records[result_idx]
                    return record
        return None

    def lookup_boundary_ordered(self, lat, lon):
        bng = self.lat_lon_to_bng(float(lat), float(lon))
        if bng:
            if self.check_point(*bng):
                result_idx = self.which_area_ordered(bng[0], bng[1])
                if result_idx:    
                    record = self.records[result_idx]
                    return record
        return None
