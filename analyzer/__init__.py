import logging
import daiquiri
import sys

daiquiri.setup(level=logging.INFO, outputs=(
    daiquiri.output.Stream(sys.stdout), daiquiri.output.File(directory='.')))
LOGGER = daiquiri.getLogger(__name__)
