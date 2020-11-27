from flask import Flask, jsonify
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
import numpy as np
import pandas as pd
import datetime as dt

#connect to the hawai sqllite
engine = create_engine("sqlite:///Resources/hawaii.sqlite",connect_args={'check_same_thread': False})
# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)
#lookup tables in the model
Base.classes.keys()
# Save reference to the table
Measurement = Base.classes.measurement
Station = Base.classes.station
# Create our session (link) from Python to the DB
session = Session(engine)
first_date = session.query(Measurement.date).order_by((Measurement.date)).limit(1).all()
print(first_date[0][0])
last_date = session.query(Measurement.date).order_by((Measurement.date).desc()).limit(1).all()
print(last_date[0][0])
last_12mnth = (dt.datetime.strptime(last_date[0][0], '%Y-%m-%d') - dt.timedelta(days=365)).date()
print(last_12mnth)
#################################################
# Flask Setup
#################################################
app=Flask(__name__)
#################################################
# Flask Routes
#################################################
#Have the home page return the information of the different routes
@app.route("/")
def welcome():
	#List all routes that are available in the home page
    return (
        f"<p>Welcome to the Hawaii weather API!!!!!!</p>"
        f"<p>All routes available:</p>"
        f"/api/v1.0/precipitation<br/>Returns a JSON representation of precipitation data for the dates between {last_12mnth} and {last_date[0][0]}<br/><br/>"
        f"/api/v1.0/stations<br/>Returns a JSON list of the weather stations<br/><br/>"
        f"/api/v1.0/tobs<br/>Returns a JSON list of the Temperature Observations (tobs) for each station for the dates between {last_12mnth} and {last_date[0][0]}<br/><br/>"
        f"/api/v1.0/yourstartdate(yyyy-mm-dd)<br/>Returns a JSON list of the minimum temperature, the average temperature, and the max temperature for the dates from the given start date in yyyy-mm-dd format <br/><br/>."
        f"/api/v1.0/start_date(yyyy-mm-dd)/end_date(yyyy-mm-dd)<br/>Returns a JSON list of the minimum temperature, the average temperature, and the max temperature for the dates between the given start date and end date<br/><br/>."
    )
#########################################################################################

@app.route("/api/v1.0/precipitation")
def precipitation():
    print("precipitation status:OK")
    #query to retrieve the last 12 months of precipitation data.
    results = session.query(Measurement.date, Measurement.prcp).filter(Measurement.date >= last_12mnth)\
        .filter(Measurement.station == Station.station).all()
    #Convert the query results to a Dictionary using date as the key and precipitation as the value.
    prcp_totals = []
    for result in results:
        row = {}
        row["date"] = result[0]
        row["prcp"] = result[1]
        prcp_totals.append(row)
    #Return the JSON representation of your dictionary.
    return jsonify(prcp_totals)
#########################################################################################

@app.route("/api/v1.0/stations")
def station():
	print("station status:OK")
	#query to get the stations.
	stations_query = session.query(Station.name, Station.station)
	stations = pd.read_sql(stations_query.statement, stations_query.session.bind)
	#Return a JSON list of stations from the dataset.
	return jsonify(stations.to_dict())
#########################################################################################

@app.route("/api/v1.0/tobs")
def tobs():
	print("tobs status:OK")
	#query for the dates and temperature observations from a year from the last data point
	tobs_results = session.query(Measurement.date, Measurement.tobs).\
	filter(Measurement.date >= last_12mnth).order_by(Measurement.date).all()
	# Create a list of dicts with `date` and `tobs` as the keys and values
	tobs_totals = []
	for result in tobs_results:
		row = {}
		row["date"] = result[0]
		row["tobs"] = result[1]
		tobs_totals.append(row)
	#Return a JSON list of Temperature Observations (tobs) for the previous year.
	return jsonify(tobs_totals)
#########################################################################################

@app.route("/api/v1.0/<start>")
def start(start):
	session = Session(engine)
# Format to input
	start_dt = str(start)
	D = (
		session
    	.query(func.min(Measurement.tobs), func.max(Measurement.tobs), func.avg(Measurement.tobs))
    	.filter(Measurement.date>=start_dt)
    	.all()
    	)
# Dictionary and jsonify
	start_list = []

	for minimal, maximal, average in D:
		D_dict = {}
		D_dict["Minimal Temperature"] = minimal
		D_dict["Maximal Temperature"] = maximal
		D_dict["Average Temperature"] = average
		start_list.append(D_dict)
  
	session.close()
	print ("Server received a GET request from a start date ...")
	return jsonify(start_list)

#########################################################################################

@app.route("/api/v1.0/<start>/<end>")
def start_end(start, end):
	session = Session(engine)
	start_dt = str(start)
	end_dt = str(end)
	E = (
		session
		.query(func.min(Measurement.tobs), func.max(Measurement.tobs), func.avg(Measurement.tobs))
		.filter(Measurement.date>=start_dt)
		.filter(Measurement.date<=end_dt)
		.all()
		)
# Dictionary and jsonify
	start_end_list = []

	for minimal, maximal, average in E:
		E_dict = {}
		E_dict[f'Minimal temperature from {start} to {end}'] = minimal
		E_dict[f'Maximal temperature from {start} to {end}'] = maximal
		E_dict[f'Average temperature from {start} to {end}'] = average
		start_end_list.append(E_dict)
  
	session.close()
	print ("Server received a GET request from a start to end date ...")
	return jsonify(start_end_list)

if __name__ == "__main__":
    app.run(debug=True)