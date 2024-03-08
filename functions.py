#### Import dependencies
import os
import yaml
import logging
import pathlib
import pandas as pd
import numpy as np
import pickle


def read_config(args):
    # read_config:
    #
    # Reads a given config yaml file, whose filename is passed as an element
    # of args. Returns the configuration of the run extracted from the
    # config yaml if it could be read.
    configfile = pathlib.Path(args.config)

    if configfile.exists():
      logging.info("Reading config file  %s", configfile)
      try:
         with configfile.open(mode="r") as file:
            config = yaml.load(file, Loader=yaml.SafeLoader)
      except OSError as exc1:
        logging.exception("Reading from file %s failed.", str(configfile))
        exit(1)
      except yaml.YAMLError as exc2:
        logging.exception("Error while parsing YAML file")
        exit(2)
    else:
      logging.error("Config file %s does not exist. Exiting.", configfile)
      exit(3)

    return config

def data_quality_summary(df):
  logging.info("Data quality:")
  #logging.info("Data column names: ")
  #logging.info(list(df.columns))

  # Total number of rows and columns
  rows = df.shape[0]
  cols = df.shape[1]
  logging.info("Number of rows: %s", rows)
  logging.info("Number of columns: %s", cols)
  # Number of rows with at least one NaN
  rows_with_NaNs = (df.isna().any(axis=1).sum())
  logging.info("Number of records with at least one NaN: %s", rows_with_NaNs)
  logging.info("Percentage of records with at least one NaN: %s", rows_with_NaNs/rows*100)

  columns_with_NaNs = (df.isna().any(axis=0).sum())
  logging.info("Number of columns with at least one NaN: %s", columns_with_NaNs)
  logging.info("Percentage of columns with at least one NaN: %s", columns_with_NaNs/cols*100)

  # Number of records with all NaNs
  logging.info("Number of records with all NaNs: %s", df.isna().all(axis=1).sum())
  logging.info("Percentage of records with all NaNs: %s", df.isna().all(axis=1).sum()/rows*100)

  return 0

def resample_dataframe(df, start, end, freq):
  resample_index = pd.date_range(start=start, end=end, freq=freq)
  dummy_frame = pd.DataFrame(np.NaN, index=resample_index, columns=df.columns)
  interpolated_frame=df.combine_first(dummy_frame).interpolate()
  interpolated_frame = interpolated_frame.loc[resample_index].drop_duplicates()
  return interpolated_frame

def read_dataset_1_file(file):
  if file.exists():
    # Read data into a dataframe and get the column names
    raw_data = pd.read_csv(file, sep=",", header=0)
    column_names = raw_data.columns
    logging.info("Successfully accessed file.")
  else:
    return null

  # Different files have different labels for the observation time
  # column. Rename them all to "time" and convert strings into datetime
  # format
  if "t_local" in column_names:
    raw_data["t_local"] = pd.to_datetime(raw_data["t_local"], utc=True)
    raw_data.rename(columns={"t_local": "time"}, inplace=True)
  elif "timestamp" in column_names:
    raw_data["timestamp"] = pd.to_datetime(raw_data["timestamp"], utc=True)
    raw_data.rename(columns={"timestamp": "time"}, inplace=True)
  else:
    logging.error("No time column found in %s. Exiting.", file)
    exit(4)

  # Drop any rows that are all NaNs
  raw_data.drop(raw_data.index[raw_data.isna().all(axis=1)], axis=0, inplace=True)

  # Set the "time" column as the index
  raw_data.set_index("time", inplace=True)

  # Drop the "Unnamed" column
  if 'Unnamed: 0' in column_names:
    raw_data.drop(columns=['Unnamed: 0'], inplace=True)
    #logging.info(raw_data.columns)

  # Sort on the index - surprisingly this is not always the case
  raw_data.sort_index(inplace=True)

  # Print out some measures of data quality
  #data_quality_summary(raw_data)

  # Interpolate the time series onto a single time index sampled at 10
  # minute intervals
  start = raw_data.index[0]
  end = raw_data.index[-1]
  freq = '600s'
  logging.info("Interpolating and resampling data.")
  interpolated_data  = resample_dataframe(raw_data, start, end, freq)
  del raw_data

  # Calculate the ensemble average for the wind farm
  interpolated_data["Mean"] = interpolated_data.mean(axis=1)

  return interpolated_data


def process_dataset_1(config, measurements=['speed']):
  # Process dataset 1 (low frequency data)

  # For each windfarm in the low frequency data set extract the windspeed
  # into a dataframe and standardise the column names, index etc
  for windfarm_id, windfarm_name  in config["dataset1"]["windfarms"].items():
    logging.info("\nProcessing windfarm id %s", windfarm_id)

    if 'speed' in measurements:
      filename = config["dataset1"]["wind"]["filenames"][windfarm_id]
      file = pathlib.Path(config["data_folder"], config["dataset1"]["subfolder"],
                     config["dataset1"]["wind"]["subfolder"],
                     filename)
      logging.info("Processing windspeed data file %s.", file)
      interpolated_data = read_dataset_1_file(file)

      # Write the data to a new file
      filename = "windspeed-"+windfarm_id.lower()+".pkl"
      file = pathlib.Path(config["output_folder"], config["dataset1"]["subfolder"],
                            filename)
      with open(file, "wb") as f:
        pickle.dump(interpolated_data, f)
      del interpolated_data
      logging.info("Formatted data written to %s\n",filename)

    if 'direction' in measurements:
      # For each windfarm in the low frequency data set extract the direction
      # into a dataframe and standardise the column names, index etc
      filename = config["dataset1"]["direction"]["filenames"][windfarm_id]
      file = pathlib.Path(config["data_folder"], config["dataset1"]["subfolder"],
                     config["dataset1"]["direction"]["subfolder"],
                     filename)
      logging.info("Processing direction data file %s.", file)
      interpolated_data = read_dataset_1_file(file)

      # Write the data to a new file
      filename = "direction-"+windfarm_id.lower()+".pkl"
      file = pathlib.Path(config["output_folder"], config["dataset1"]["subfolder"],
                            filename)
      with open(file, "wb") as f:
        pickle.dump(interpolated_data, f)
      del interpolated_data
      logging.info("Formatted data written to %s\n",filename)

    if 'power' in measurements:
      # For each windfarm in the low frequency data set extract the power
      # into a dataframe and standardise the column names, index etc
      filename = config["dataset1"]["power"]["filenames"][windfarm_id]
      file = pathlib.Path(config["data_folder"], config["dataset1"]["subfolder"],
                     config["dataset1"]["power"]["subfolder"],
                     filename)
      logging.info("Processing power data file %s.", file)
      interpolated_data = read_dataset_1_file(file)

      # Write the data to a new file
      filename = "power-"+windfarm_id.lower()+".pkl"
      file = pathlib.Path(config["output_folder"], config["dataset1"]["subfolder"],
                            filename)
      with open(file, "wb") as f:
        pickle.dump(interpolated_data, f)
      del interpolated_data
      logging.info("Formatted data written to %s\n",filename)
  return 0


def process_dataset_2(config):
   # Process dataset 2 (high frequency data)

  # This data set contains about 10 days of data for 2 turbines sampled at
  # approximately 5 second intervals. It includes wind speed, wind direction
  # and power. The data is split into 3 files containing 3 consecutive
  # data ranges which needs to be joined together. Each file contains
  # the data for turbine 1 followed by the data for turbine 2 which need to
  # be separated out into 2 columns. The sampling rate is also slightly
  # irregular which needs to be corrected for.

  # Read in the 3 separate raw data files into a dictionary of dataframes
  raw_data_0={}
  original_column_names={}
  for i, filename in enumerate(config["dataset2"]["filenames"]):
      file = pathlib.Path(config["data_folder"], config["dataset2"]["subfolder"],
                          filename)
      if file.exists():
        logging.info("Reading file: %s", filename)
        sheet = config["dataset2"]["excel_sheet_names"]["wind"]
        raw_data_0[i] = pd.read_excel(file, sheet_name=sheet,skiprows=0)
        #original_column_names[i] = raw_data_0[i].columns
        #logging.info(raw_data_0[i].columns)

  # For each data file, separate the data for turbines 1 and 2 and concat
  # the 3 date ranges together to create a single dataframe for each
  # turbine
  raw_data_1 = {}
  # Loop over the 2 wind turbines
  for turbine in [1,2]:
    logging.info("Recombining data for turbine %s",str(turbine))
    raw_data_1[turbine] = pd.DataFrame()
    # Loop over the 3 date ranges for each
    for i in [0,1,2]:
        select_rows = raw_data_0[i]["WTG"]== turbine
        df = raw_data_0[i][select_rows]
        raw_data_1[turbine] = pd.concat([raw_data_1[turbine], df])

    # We can now drop the "WTG" column after the split
    raw_data_1[turbine].drop(columns=['WTG'], inplace=True)

    # Rename the remaining columns to standardise names
    new_column_names = ["time","speed","direction","power"]
    relabel = dict(zip(raw_data_1[turbine].columns.to_list(), new_column_names))
    raw_data_1[turbine].rename(columns=relabel, inplace=True)

    # Set the "time" column as the index
    raw_data_1[turbine].set_index("time", inplace=True, drop=True)

    # Print out some measures of data quality
    data_quality_summary(raw_data_1[turbine])

  # No longer need raw_data_0
  del raw_data_0


  # Interpolate the time series from the two turbines onto a single time index
  # sampled at 5 second intervals

  interpolated_data = {}
  start = raw_data_1[1].index[0]
  end = raw_data_1[1].index[-1]
  freq = '5s'
  for turbine in [1,2]:
    logging.info("Interpolating and resampling data for turbine %s",str(turbine))
    interpolated_data[turbine] = resample_dataframe(raw_data_1[turbine], start, end, freq)

  # We no longer need raw_data_1
  del raw_data_1

  # Regroup the data to create separate frames containing speed, direction and
  # power timeseries for both turbines.
  idx = interpolated_data[1].index
  speed = pd.DataFrame(0, index=idx, columns=["WTG1","WTG2"])
  direction = pd.DataFrame(0, index=idx, columns=["WTG1","WTG2"])
  power = pd.DataFrame(0, index=idx, columns=["WTG1","WTG2"])
  for t in [1,2]:
    speed["WTG"+str(t)]=interpolated_data[t]["speed"]
    direction["WTG"+str(t)]=interpolated_data[t]["direction"]
    power["WTG"+str(t)]=interpolated_data[t]["power"]

  # Write processed data to files
  filename = "windspeed-hwrd.pkl"
  file = pathlib.Path(config["output_folder"], config["dataset2"]["subfolder"],
                            filename)
  logging.info("Writing processed wind speed data to %s", filename)
  with open(file, "wb") as f:
    pickle.dump(speed, f)
  filename = "direction-hwrd.pkl"
  file = pathlib.Path(config["output_folder"], config["dataset2"]["subfolder"],
                            filename)
  logging.info("Writing processed wind direction data to %s", filename)
  with open(file, "wb") as f:
    pickle.dump(direction, f)
  filename = "power-hwrd.pkl"
  file = pathlib.Path(config["output_folder"], config["dataset2"]["subfolder"],
                            filename)
  logging.info("Writing processed power data to %s", filename)
  with open(file, "wb") as f:
    pickle.dump(power, f)


