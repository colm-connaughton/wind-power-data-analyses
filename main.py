import sys
import argparse
import pathlib
import logging
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


    functions.process_dataset_1(config)

    #functions.process_dataset_2(config)



### On being called execute the main() function

if __name__ == "__main__":
    main()
