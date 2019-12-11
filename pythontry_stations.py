from pymongo import MongoClient
from pprint import pprint
from datetime import datetime

def get_detector(line):
	line = line.split(',')
	detectorid = int(line[0])
	detectorclass = int(line[4])
	lanenumber = int(line[5])
	stationid = int(line[6].strip())

	# print("detectorid", detectorid)
	# print("highwayid", highwayid)
	# print("milepost", milepost)
	# print("locationtext", locationtext)
	# print("detectorclass", detectorclass)
	# print("lanenumber", lanenumber)
	# print("stationid", stationid)

	row = {
	'detectorid': detectorid,
	'detectorclass': detectorclass,
	'lanenumber': lanenumber
	}

	return [stationid, row]

def get_station(line):
	line = line.split(',')
	#print(line)
	stationid = int(line[0])
	milepost = float(line[1])
	locationtext = line[2]
	upstream = int(line[3])
	downstream = int(line[4])
	stationclass = int(line[5])
	numberlanes = int(line[6])
	latlon = line[7].replace("~", ",", 1)
	length = float(line[8])
	highway_shortdirection = line[9]
	highway_direction = line[10]
	highwayname = line[11].strip()
	# print("stationid ", stationid)
	# print("milepost ", milepost)
	# print("locationtext ", locationtext)
	# print("upstream ", upstream)
	# print("downstream ", downstream)
	# print("stationclass ", stationclass)
	# print("numberlanes ", numberlanes)
	# print("latlon ", latlon)
	# print("length ", length)
	# print("highway_shortdirection ", highway_shortdirection)
	# print("highway_direction ", highway_direction)
	# print("highwayname ", highwayname)
	row = {
		'stationid':stationid,
		'milepost':milepost,
		'locationtext':locationtext,
		'upstream':upstream,
		'downstream':downstream,
		'stationclass':stationclass,
		'numberlanes':numberlanes,
		'latlon':latlon,
		'length':length,
		'highway_shortdirection':highway_shortdirection,
		'highway_direction':highway_direction,
		'highwayname':highwayname
	}
	#print(row)
	return row

def read_csv(memory, csvfile):
	count = 0
	error = 0

	fileHandler = open(csvfile, "r")#, encoding="utf-8")
	for line in fileHandler:
		#try:
		count += 1
		line = line.encode('ascii', 'ignore').decode('ascii')

		if type(memory) is list:
			row = get_station(line)
			memory.append(row)
		else:
			stationid, detector = get_detector(line)
			if stationid not in memory:
				memory[stationid] = []
			memory[stationid].append(detector)
		#except:
		#	error += 1
	fileHandler.close()
	print('count', count)
	print('error', error)
	return memory


if __name__ == "__main__":
	
	client = MongoClient("mongodb://127.0.0.1:27017")
	db=client.test
	serverstatus = db.command("serverStatus")

	stations = []
	stations = read_csv(stations, 'freeway_stations.csv')
	detectors = {}
	detectors = read_csv(detectors, 'freeway_detectors.csv')
	print(len(stations))
	print(len(detectors))
	#with open("station.json", "a") as write_file:
	for station in stations:
		stationid = station['stationid']
		if stationid in detectors:
			station['detectors'] = detectors[stationid]
		else:
			station['detectors'] = []
		#json.dump(station, write_file)
		result = db.stations.insert_one(station)