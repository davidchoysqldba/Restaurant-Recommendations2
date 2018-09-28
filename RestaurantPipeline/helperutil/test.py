import unittest
import logging
from file_date_util import getdownloadedNthFiles

log = logging.getLogger(__name__)


result = getdownloadedNthFiles('aaa', 'bbb')
print(result)

