#### Import dependencies
import os
import yaml
import logging
import pathlib



def read_config(args):
    # read_config:
    #
    # Reads a given config yaml file, whose filename is passed as an element
    # of args. Returns the configuration of the run extracted from the
    # config yaml if it could be read.
    configfile = pathlib.Path(args.config)

    if configfile.exists():
      logging.info("Reading config file ", str(configfile), "\n")
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
