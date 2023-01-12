import logging


def set_logger():

    logging.basicConfig()

    logging.getLogger().setLevel(logging.INFO)

    return logging


set_logger()