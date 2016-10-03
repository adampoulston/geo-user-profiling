import json
import sqlite3
import numpy as np
from scipy.cluster.vq import kmeans2, whiten, kmeans, vq
from scipy.spatial.distance import pdist
from scipy.cluster.hierarchy import linkage, dendrogram, fcluster
from scipy.stats import *
from collections import Counter
from datetime import datetime
from scipy.optimize import minimize
from scipy.spatial.distance import cdist
import sys
from haversine import haversine

def apply_haversine(c1, c2, miles=True):
	return haversine(c1, c2, miles)

def kmeans_num_clusters_jump_method(X, n):
	Y = float(X[0].shape[0]) / 2.0
	# print Y
	D = [0] * n
	distortions = []
	for k in range(0, n):
		centroids, distortion = kmeans(X, k+1, iter = 20)
		d = distortion
		D[k] = np.power(d, -Y)
		# D[k] = d
		distortions.append([k, D[k]])
	J = np.array([D[i] - D[i-1] for i in range(1,n)])
	dist = np.array(distortions)

	# J = np.array([D[i+1]+D[i]-D[i-1] for i in range(1,n-1)])
	# print J
	return (np.argmax(J) + 1, dist)

def geometric_median(points):
    points = np.asarray(points)
    # objective function
    def aggregate_distance(x):
        return cdist([x], points).sum()

    # initial guess: centroid
    centroid = points.mean(axis=0)

    optimize_result = minimize(aggregate_distance, centroid, method='COBYLA')

    return optimize_result.x

class HomeLocationAssigner(object):
	def assign_home(self, user_tweets, centroid_type="med"):
		coordinates = np.array([t['coordinates']['coordinates'] for t in user_tweets if t['coordinates']])
		if len(coordinates) <= 1:
			raise Exception("More than one geolocated tweet required.")
		#Use users latest user object
		user = user_tweets[0]['user']
		
		home_info = {}

		n_clusters = 10
		#run kmeans
		x, y = kmeans2(coordinates, n_clusters, iter = 20, minit='points')
		#get the most populous cluster and set it as home
		cluster_counts = Counter(y).most_common()
		home_cluster = cluster_counts[0][0]
		#extract point in home cluster
		hc_members = coordinates[(y == home_cluster).nonzero()]

		#home coordinate as average (kmeans default)
		if centroid_type == "avg":
			home_coord = np.array(x[home_cluster] ,np.float32)
			distances_to_hc_avg = cdist(hc_members, [home_coord], apply_haversine)
			home_info['hc'] = list(home_coord)
			home_info['hc-med-density'] = np.median(distances_to_hc_avg)
			home_info['hc-mean-density'] = np.mean(distances_to_hc_avg)

		#home coordinate as the geometric median of home cluster members
		if centroid_type == "med":
			med = geometric_median(hc_members)
			distances_to_hc_med = cdist(hc_members, [med], apply_haversine)
			home_info['hc'] = list(med)
			home_info['hc-med-density'] = np.median(distances_to_hc_med)
			home_info['hc-mean-density'] = np.mean(distances_to_hc_med)		


		
		###############Non-individual profile filtering
		#Give profile a point for each heuristic it hits
		points = 0

		#Key phrase filtering
		phrases = ["we offer", "contact us", "dealership", "we bring", 
			"our website", "we are", "we help", "opportunities", "can help", 
			"we ship", "range of services", "official twitter", "we have a passion", 
			"follow us on", "look no further", "offers", "follow for", "for updates"
			"subscribe for", "subscribe to", "documenting", "we create",
			"we sell", "visit our", "selling", "award winning", "free download",
			"check out our", "to book", "we stock", "services", "we bring", "free shipping",
			"free uk shipping", "international company", "book now", "as seen on", "makers of",
			"get in touch", "venue hire", "specialising in", "for more", "visit us",
			"buy now"]
		for phrase in phrases:
			if user['description']:
				if phrase in user['description'].lower():
					# print phrase + " : " + users[user]['description']
					points += 1


		#H1 - One suspiciously dense cluster
		# if n_clusters == 1:
		# 	points_in_clusters = []
		# 	for i in range(0,np.unique(y).shape[0]):
		# 		points_in_clusters.append([])
		# 	for idx,cluster in enumerate(y):
		# 		points_in_clusters[cluster].append((coordinates[idx][0],coordinates[idx][1]))
		# 	cluster_means = []
		# 	cluster_stds = []
		# 	for idx, coord in enumerate(x):
		# 		cluster_means.append(np.mean(cdist([coord], points_in_clusters[idx])))
		# 		cluster_stds.append(np.std(cdist([coord], points_in_clusters[idx])))
		# 	if cluster_means[home_cluster] < 0.01:
		# 		points += 1
		# 		# print cluster_stds
		# 		# print cluster_means
				

		#H2 - suprisingly consistent posting habits
		hour_hist = [0] * 24
		hours = []
		for tweet in user_tweets:
			tweet['created_at'] = datetime.strptime(tweet['created_at'], "%a %b %d %H:%M:%S +0000 %Y")

			hour_hist[tweet['created_at'].hour] += 1
			hours.append(float(tweet['created_at'].hour) + (float(tweet['created_at'].minute)/60.0))
		hours_mean = circmean(hours, high=23.999, low=0.0)
		hours_std = circstd(hours, high=23.999, low=0.0)

		#catches weather bots and other consistent posters
		if hours_std > 11.0:
			points += 1

		#doesnt do much, but does identify some work-only posters
		# if hours_std < 4.0:
		# 	points += 1

		#H3 - suspicious follower/firend ratio
		try:
			rep = user['followers_count'] / (user['followers_count'] + user['friends_count'])
		except:
			rep = 0
		#0.93 expertly chosen arbritarily
		if rep > 0.93:
			points += 1

		home_info['suspicion_points'] = points

		return home_info
