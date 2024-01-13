import sys
import argparse
import pathlib
import functions


def main():
    # main:
    #
    # Runs the portions of the pipeline as determiend by the passed config
    # file, or config_default.yaml if none passed.
    parser = argparse.ArgumentParser()
    parser.add_argument("config", help="yaml file containing configuration information and data layout")
    args = parser.parse_args()

    # Read the passed config file
    config = functions.read_config(args)

    locations = config["dataset1"]["locations"]
    keys = config["dataset1"]["locations"].keys()
    for k in keys:
        print(k, locations[k])
    for k in keys:
        filename = k.lower()+"_windspeed_COD_to_20230601.csv"
        file = pathlib.Path(config["data_folder"], config["dataset1"]["subfolder"],
                     config["dataset1"]["wind"]["subfolder"],
                     filename)
        if file.exists():
          print(file, "exists.")

### On being called execute the main() function

if __name__ == "__main__":
    main()
