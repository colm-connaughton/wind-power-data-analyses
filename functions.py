#### Import dependencies
import os
import yaml
import logging
import pathlib
import pandas as pd
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

def process_dataset_1(config):
   # Process dataset 1 (low frequency data)

    # Create a dictionary of dataframes for the windspeed data from each
    # wind farm in the low frequency data files
    windspeed={}

    # For each windfarm in the low frequency data set extract the windspeed
    # into a dataframe and standardise the column names, index etc
    for windfarm_id, windfarm_name  in config["dataset1"]["windfarms"].items():
        filename = windfarm_id.lower()+"_windspeed_COD_to_20230601.csv"
        file = pathlib.Path(config["data_folder"], config["dataset1"]["subfolder"],
                     config["dataset1"]["wind"]["subfolder"],
                     filename)
        if file.exists():
          logging.info("Processing data file %s.", file)
          # Read data into a dataframe and get the column names
          windspeed[windfarm_id] = pd.read_csv(file, sep=",", header=0)
          column_names = windspeed[windfarm_id].columns

          # Different files have different labels for the observation time
          # column. Rename them all to "time" and convert strings into datetime
          # format
          if "t_local" in column_names:
            windspeed[windfarm_id]["t_local"] = pd.to_datetime(windspeed[windfarm_id]["t_local"], utc=True)
            windspeed[windfarm_id].rename(columns={"t_local": "time"}, inplace=True)
          elif "timestamp" in column_names:
            windspeed[windfarm_id]["timestamp"] = pd.to_datetime(windspeed[windfarm_id]["timestamp"], utc=True)
            windspeed[windfarm_id].rename(columns={"timestamp": "time"}, inplace=True)
          else:
            logging.error("No time column found in %s. Exiting.", file)
            exit(4)

          # Set the "time" column as the index
          windspeed[windfarm_id].set_index("time", inplace=True)

          # Drop the "Unnamed" column
          if 'Unnamed: 0' in column_names:
            windspeed[windfarm_id].drop(columns=['Unnamed: 0'], inplace=True)
          logging.info(windspeed[windfarm_id].columns)

          # Check if there are any NaNs and drop any rows containing NaNs
          n = windspeed[windfarm_id].isnull().sum().sum()
          if n>0:
            logging.warning("%s NaNs found in %s.", n, file)
            windspeed[windfarm_id].dropna(inplace=True, axis=0)

          # Calculate the ensemble average for the wind farm
          windspeed[windfarm_id]["Mean"] = windspeed[windfarm_id].mean(axis=1)

          # Write the data to a new file
          filename = "windspeed-"+windfarm_id.lower()+".pkl"
          file = pathlib.Path(config["output_folder"], config["dataset1"]["subfolder"],
                            filename)
          with open(file, "wb") as f:
            pickle.dump(windspeed[windfarm_id], f)

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
        logging.info("Processing file: %s", filename)
        sheet = config["dataset2"]["excel_sheet_names"]["wind"]
        raw_data_0[i] = pd.read_excel(file, sheet_name=sheet,skiprows=0)
        original_column_names[i] = raw_data_0[i].columns
        logging.info(raw_data_0[i].columns)

  # For each data file, separate the data for turbines 1 and 2 and concat
  # the 3 date ranges together to create a single dataframe for each
  # turbine
  raw_data_1 = {}
  # Loop over the 2 wind turbines
  for turbine in [1,2]:
    raw_data_1[turbine] = pd.DataFrame()
    # Loop over the 3 date ranges for each
    for i in [0,1,2]:
        select_rows = raw_data_0[i]["WTG"]== turbine
        df = raw_data_0[i][select_rows]
        raw_data_1[turbine] = pd.concat([raw_data_1[turbine], df])
    # We can now drop the "WTG" column after the split
    raw_data_1[turbine].drop(columns=['WTG'], inplace=True)
  # No longer need raw_data_0
  del raw_data_0

# Interpolate the time series from the two turbines onto a single time index
# sampled at 5 second intervals

# This code seems to work in the sandpit
#df = df1[1].set_index(pd.to_datetime(df1[1]['time_stamp'], unit='s'), drop=False)
#(start, end) = (df.index[0], df.index[-1])
#resample_index = pd.date_range(start=start, end=end, freq='5s')
#dummy_frame = pd.DataFrame(np.NaN, index=resample_index, columns=df.columns)
#df.combine_first(dummy_frame).interpolate().iloc[:6]

