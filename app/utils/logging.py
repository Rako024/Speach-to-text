import logging

def setup_logging():
    logging.basicConfig(...)
    logging.getLogger('uvicorn').setLevel(logging.INFO)