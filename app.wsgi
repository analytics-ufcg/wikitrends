import sys
sys.path.insert(0, "/home/guilhermemg/gadbi/wikitrends")

import logging
logging.basicConfig(stream=sys.stderr)

from gadbi_api import app as application
