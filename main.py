import sys
import argparse
import pathlib
import logging
import pandas as pd
import pickle
import functions



def main():
    # main:
    #
    # Runs the portions of the pipeline as determiend by the passed config
    # file, or config_default.yaml if none passed.
    parser = argparse.ArgumentParser()
    parser.add_argument("config", help="yaml file containing configuration information and data layout")
    parser.add_argument("--verbose", help="increase output verbosity",
                    action="store_true")
    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.INFO)

    # Read the passed config file
    config = functions.read_config(args)

    locations = config["dataset1"]["locations"]
    keys = config["dataset1"]["locations"].keys()

    windspeed={}
    for k in keys:
        filename = k.lower()+"_windspeed_COD_to_20230601.csv"
        file = pathlib.Path(config["data_folder"], config["dataset1"]["subfolder"],
                     config["dataset1"]["wind"]["subfolder"],
                     filename)
        if file.exists():
          logging.info("Processing data file %s.", file)
          # Read data into a pandas dataframe
          windspeed[k] = pd.read_csv(file, sep=",", header=0)
          column_names = windspeed[k].columns
          if "t_local" in column_names:
            windspeed[k]["t_local"] = pd.to_datetime(windspeed[k]["t_local"], utc=True)
            windspeed[k].rename(columns={"t_local": "time"}, inplace=True)
          elif "timestamp" in column_names:
            windspeed[k]["timestamp"] = pd.to_datetime(windspeed[k]["timestamp"], utc=True)
            windspeed[k].rename(columns={"timestamp": "time"}, inplace=True)
          else:
            logging.error("No time column found in %s. Exiting.", file)
            exit(4)
          windspeed[k].set_index("time", inplace=True)
          if 'Unnamed: 0' in column_names:
            windspeed[k].drop(columns=['Unnamed: 0'], inplace=True)
          logging.info(windspeed[k].columns)

          # Check if there are any NaNs
          n = windspeed[k].isnull().sum().sum()
          if n>0:
            logging.warning("%s NaNs found in %s.", n, file)
            # Drop rows containing NaNs
            windspeed[k].dropna(inplace=True, axis=0)

          # Write the data to a new file
          filename = "windspeed-"+k.lower()+".pkl"
          file = pathlib.Path(config["output_folder"], config["dataset1"]["subfolder"],
                            filename)
          with open(file, "wb") as f:
            pickle.dump(windspeed[k], f)



### On being called execute the main() function

if __name__ == "__main__":
    main()
